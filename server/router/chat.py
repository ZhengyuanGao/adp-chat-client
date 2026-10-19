import asyncio
import logging
import sanic
from sanic.exceptions import SanicException
from sanic.request.types import Request
from sanic.response import ResponseStream
from sanic.views import HTTPMethodView
from sanic_restful_api import reqparse

from app_factory import TAgenticApp
from core.chat import CoreChat
from core.conversation import CoreConversation
from core.share import CoreShareConversation
from core.account import CoreAccount
from router import login_required, check_login
from util.database import db_connection

app: TAgenticApp = TAgenticApp.get_app()


async def _build_extra_custom_variables(account_id: str) -> dict:
    """从 DB 读取账号信息，构造要注入 ADP CustomVariables 的额外字段。

    重要约定：
    - /chat/message 走 SSE，中间件不注入 request.ctx.db，必须自己借还连接
    - 借用范围要"最小化"，读完立刻还，绝不能跨越 SSE 流生命周期
    - ADP 侧 CustomVariables 的 value 必须是 string，dict/list 需 json.dumps
    """
    import json  # 局部导入，避免影响模块加载顺序
    async with db_connection() as db:
        account = await CoreAccount.get(db, account_id)
    if account is None:
        raise SanicException("account not found", status_code=401)

    extra: dict = {
        "account": json.dumps({
            "VisitorUserAccount": account.Name or "",
            "ctSys": account.ExtraInfo.ctSys or "kwportal"
        }, ensure_ascii=False),
    }
    # 如果 ExtraInfo 里还有自定义字段，需要平铺给 ADP，可在此处补充
    return extra

class ChatMessageApi(HTTPMethodView):
    @login_required
    async def post(self, request: Request):
        parser = reqparse.RequestParser()
        parser.add_argument("Contents", type=list, required=True, location="json")
        parser.add_argument("ConversationId", type=str, location="json")
        parser.add_argument("ApplicationId", type=str, location="json")
        parser.add_argument("SearchNetwork", type=bool, default=True, location="json")
        parser.add_argument("CustomVariables", type=dict, default={}, location="json")
        # 渠道会话（企微 / 微信 Bot 等）：vendor 侧才是权威数据源，
        # 本地不落地 chat_conversation，避免污染 /chat/conversations 侧栏列表。
        parser.add_argument("IsChannel", type=bool, default=False, location="json")
        args = parser.parse_args(request)
        logging.info(f"ChatMessageApi: {args}")

        application_id = args['ApplicationId']
        vendor_app = app.get_vendor_app(application_id)

        # 附加后端自定义参数（例如账号信息）。必须在 streaming_fn 之外完成 DB 访问：
        # /chat/message 在 middleware/database.py 的 _SKIP_SESSION_PATHS 白名单中，
        # request.ctx 上没有 db 属性，且 SSE 响应可能持续数十秒，绝不能把连接带进流里。
        args['CustomVariables'].update(
            await _build_extra_custom_variables(request.ctx.account_id)
        )

        logging.info(f"[ChatMessageApi] ApplicationId: {application_id},\n\
            CustomVariables: {args['CustomVariables']},\n\
            IsChannel: {args['IsChannel']},\n\
            vendor_app: {vendor_app}")

        async def streaming_fn(response):
            chat_gen = CoreChat.message(
                vendor_app,
                request.ctx.account_id,
                args['Contents'],
                args['ConversationId'],
                args['SearchNetwork'],
                args['CustomVariables'],
                is_channel=args['IsChannel'],
            )
            try:
                async for data in chat_gen:
                    await response.write(data)
            except asyncio.CancelledError:
                logging.info("[ChatMessageApi] Client disconnected, closing upstream")
                await chat_gen.aclose()
                raise
        return ResponseStream(streaming_fn, content_type='text/event-stream; charset=utf-8')


class ChatMessageListApi(HTTPMethodView):
    async def get(self, request: Request):
        parser = reqparse.RequestParser()
        parser.add_argument("ConversationId", type=str, required=False, location="args")
        parser.add_argument("LastRecordId", type=str, required=False, location="args")
        parser.add_argument("ShareId", type=str, required=False, location="args")
        args = parser.parse_args(request)

        if args["ConversationId"] is not None:
            check_login(request)
            application_id = await CoreConversation.get_application_id(
                request.ctx.db,
                request.ctx.account_id,
                args['ConversationId']
            )
            vendor_app = app.get_vendor_app(application_id)

            # 判断是否为 claw 模式：优先从 apps_info 缓存查找，找不到时动态获取
            is_claw = False
            for info in getattr(request.ctx, 'apps_info', []):
                if hasattr(info, 'ApplicationId'):
                    app_id = info.ApplicationId
                else:
                    app_id = info.get('ApplicationId', '')
                pattern = info.Pattern if hasattr(info, 'Pattern') else info.get('Pattern', '')
                if app_id == application_id and pattern == 'ClawAgent':
                    is_claw = True
                    break

            # 缓存中未找到该应用信息时，通过 vendor_app 动态获取 Pattern
            if not is_claw:
                def _get_app_id(info):
                    if hasattr(info, 'ApplicationId'):
                        return info.ApplicationId
                    return info.get('ApplicationId', '')

                found_in_cache = any(
                    _get_app_id(info) == application_id
                    for info in getattr(request.ctx, 'apps_info', [])
                )
                if not found_in_cache and hasattr(vendor_app, 'get_info'):
                    try:
                        app_info = await vendor_app.get_info()
                        if getattr(app_info, 'Pattern', None) == 'ClawAgent':
                            is_claw = True
                    except (OSError, ValueError, KeyError, AttributeError) as e:
                        logging.warning(
                            f'[ChatApi] get_info failed for {application_id}: {e}'
                        )

            if is_claw and hasattr(vendor_app, 'get_messages_v2'):
                # claw 模式：通过 DescribeConversationMessageList 获取完整 V2 数据
                result = await vendor_app.get_messages_v2(
                    request.ctx.db,
                    request.ctx.account_id,
                    args['ConversationId'],
                    app.config.CHAT_MESSAGE_PAGE_SIZE,
                    args['LastRecordId']
                )
                resp = {
                    'Response': {
                        'ApplicationId': application_id,
                        'Records': result['Records'],
                        'HasMoreBefore': result['HasMoreBefore'],
                        'LastRecordId': result['LastRecordId'],
                    }
                }
            else:
                # standard 模式：通过 GetMsgRecord 获取历史消息
                records = await vendor_app.get_messages(
                    request.ctx.db,
                    request.ctx.account_id,
                    args['ConversationId'],
                    app.config.CHAT_MESSAGE_PAGE_SIZE,
                    args['LastRecordId']
                )
                resp = {
                    'Response': {
                        'ApplicationId': application_id,
                        'Records': records,
                    }
                }
            return sanic.json(resp)

        if args["ShareId"] is not None:
            if args['LastRecordId'] is not None:
                # temporarily disable pagination loading for the share API
                result = []
            else:
                conversation = await CoreShareConversation.list(request.ctx.db, args["ShareId"])
                result = conversation.to_dict()
            return sanic.json({"Response": result})

        raise SanicException('ConversationId or ShareId is required')


class ChatConversationListApi(HTTPMethodView):
    @login_required
    async def get(self, request: Request):
        parser = reqparse.RequestParser()
        parser.add_argument("ApplicationId", type=str, required=False, location="args")
        args = parser.parse_args(request)

        conversations = await CoreConversation.list(
            request.ctx.db,
            request.ctx.account_id,
            application_id=args["ApplicationId"],
        )
        return sanic.json([conversation.to_dict() for conversation in conversations])


class ChatConversationDeleteApi(HTTPMethodView):
    @login_required
    async def post(self, request: Request):
        parser = reqparse.RequestParser()
        parser.add_argument("ConversationId", type=str, required=True, location="json")
        args = parser.parse_args(request)

        await CoreConversation.delete(request.ctx.db, request.ctx.account_id, args["ConversationId"])
        return sanic.json({"Success": 1})


app.add_route(ChatMessageApi.as_view(), "/chat/message")
app.add_route(ChatMessageListApi.as_view(), "/chat/messages")
app.add_route(ChatConversationListApi.as_view(), "/chat/conversations")
app.add_route(ChatConversationDeleteApi.as_view(), "/chat/conversation/delete")
