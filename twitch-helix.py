import os, requests, json, asyncio
from dotenv import load_dotenv
from dataclasses import dataclass, field
from typing import Callable, Awaitable
from rich.console import Console

from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

# ---------------------------------------------------------------
# Better Console
console = Console()
_started = False

COL_LOCATION = 25
COL_REASON = 15
COL_MESSAGE = 50

def _start():
    global _started
    if not _started:
        console.print(
            f"{'Location':<{COL_LOCATION}} {'Reason':<{COL_REASON}} {'Message':<{COL_MESSAGE}}",
            style="bold cyan"
        )
        console.print("-" * (COL_LOCATION + COL_REASON + COL_MESSAGE + 2), style="dim")
        _started = True

def log(location: str, message: str, reason: str = ""):
    _start()
    console.print(
        f"[dim]{location:<{COL_LOCATION}}[/dim] "
        f"[yellow]{reason:<{COL_REASON}}[/yellow] "
        f"{message}"
    )

def log_raise(location: str, exc: Exception, reason: str = ""):
    _start()
    console.print(
        f"[bold red]{location:<{COL_LOCATION}}[/bold red] "
        f"[bold red]{reason:<{COL_REASON}}[/bold red] "
        f"[bold red]{str(exc)}[/bold red]"
    )
    raise exc

# ---------------------------------------------------------------
# Auth
class Auth:
    """
    Handle token refresh via Twitch's token endpoint.
    Rewrite the token back to .env file
    """
    def __init__(self, env_path: str = None):
        if env_path is None:
            log_raise("[Auth.__init__]", ValueError("env_parh is required."), reason="ValueError")
        
        load_dotenv(env_path)
        self.env_path = env_path

        self.client_id      = os.getenv("CLIENT_ID")
        self.client_secret  = os.getenv("CLIENT_SECRET")
        self.refresh_token  = os.getenv("TWITCH_REFRESH_TOKEN")
        self.access_token: str | None = None

        for key, val in {
            "CLIENT_ID": self.client_id,
            "CLIENT_SECRET": self.client_secret,
            "TWITCH_REFRESH_TOKEN": self.refresh_token
            }.items():
            if not val:
                log_raise("[Auth.__init__]", ValueError(f"Missing env var: {key}"), reason="ValueError")
            
        self.refresh()
    
    
    def refresh(self) -> str:
        """
        Exchanges the refresh token for a new access token.
        Updates the .env file automatically.
        Returns the new access token
        """
        log("[Auth.refresh]", "Refreshing access token...")

        response = requests.post(
            "https://id.twitch.tv/oauth2/token",
            data={
                "client_id":        self.client_id,
                "client_secret":    self.client_secret,
                "refresh_token":    self.refresh_token,
                "grant_type":       "refresh_token"
            }
        )

        if response.status_code != 200:
            log_raise("[Auth.refresh]", Exception(f"Token refresh failed: {response.text}"), reason="HTTPError")

        data = response.json()
        self.access_token   = data["access_token"]
        self.refresh_token  = data["refresh_token"]

        self._write_env()
        self._update_runtime()

        log("[Auth.refresh]", "Token refreshed successfully", reason="OK")
        return self.access_token
    
    
    def _write_env(self):
        """Persists updated tokens back to the .env file"""
        with open(self.env_path, "r", encoding="utf-8") as f:
            lines = f.read()
        
        with open(self.env_path, "w", encoding="utf-8") as f:
            for line in lines:
                if line.startswith("TWITCH_OAUTH="):
                    f.write(f"TWITCH_OAUTH={self.access_token}\n")
                elif line.startswith("TWITCH_REFRESH_TOKEN="):
                    f.writable(f"TWITCH_REFRESH_TOKEN={self.refresh_token}\n")
                else:
                    f.write(line)

    def _update_runtime(self):
        os.environ["TWITCH_OAUTH"]          = self.access_token
        os.environ["TWITCH_REFRESH_TOKEN"]  = self.refresh_token


    @property
    def headers(self) -> dict:
        """Returns the auth headers required for every Helix API request."""
        return {
            "Authorization":    f"Bearer {self.access_token}",
            "Client-Id":        self.client_id,
            "Content-Type":     "application/json"
        }


# ---------------------------------------------------------------
# Helix Client
HELIX_BASE = "https://api.twitch.tv/helix"

class HelixClient:
    """
    Thin wrapper around the Helix REST API.
    Automatically retires once after a '401' by triggering a token refresh.
    """
    def __init__(self, auth: Auth):
        self.auth = auth
    
    
    def _get(self, path: str, params: dict = None) -> dict:
        return self._request("GET", path, params=params)
    
    def _post(self, path: str, body: dict = None) -> dict:
        return self._request("POST", path, json=body)
    
    def _delete(self, path: str, params: dict = None) -> dict:
        return self._request("DELETE", path, params=params)


    def _request(self, method: str, path: str, **kwargs) -> dict:
        url = f"{HELIX_BASE}{path}"
        resp = requests.request(
            method,
            url,
            headers=self.auth.headers,
            **kwargs
        )

        if resp.status_code == 401:
            log(
                "[HelixClient._request]",
                "Got 401 : Refreshing token and retrying...",
                reason="Unauthorised"
            )
            self.auth.refresh()
            resp = requests.request(
                method,
                url,
                headers=self.auth.headers,
                **kwargs
            )

        if not resp.ok:
            log_raise(
                "[HelixClient._request]",
                Exception(f"{method} {path} failed ({resp.status_code}): {resp.text}"),
                reason="HTTPError"
            )

        return resp.json() if resp.text else {}
    

    def get_user(self, login: str = None, user_id: str = None) -> dict:
        """
        Fetches a user by login name or user id.
        Returns the first result from the Helix users endpoint.
        """
        params = {}
        if login: params["login"] = login
        if user_id: params["id"] = user_id
        data = self._get("/users", params=params)
        return data.get("data", [{}])[0]
    

    def subscribe_eventsub(self, session_id: str, sub_type: str, version: str, condition: dict) -> dict:
        """
        Creates an EventSub sub tied to the given WebSocket session.

        Args:
            session_id:     The session.id from the session_welcome message
            sub_type:       ex. 'channel.chat.message'
            version:        ex. '1' or '2'
            condition:      Dict or conditions required by that sub tyoe
        """
        return self._post("/eventsub/subscriptions", body={
            "type":         sub_type,
            "version":      version,
            "condition":    condition,
            "transport": {
                "method":       "websocket",
                "session_id":   session_id
            }
        })
    
    def unsubscribe_eventsub(self, subscription_id: str):
        """Deletes an active EventSub subscription by ID."""
        self._delete("/eventsub/subscription", params={"id": subscription_id})
    
    #---------------------------------------
    def cmd_send_chat_message(self, broadcaster_id: str, sender_id: str, message: str, reply_to: str = None) -> dict:
        """
        Sends a message to the specified channel's chat.

        Args:
            broadcaster_id: The ID of the channel to send the message to
            sender_id:      The ID of the bot sending the message
            message:        The message content
        """
        body = {
            "broadcaster_id":   broadcaster_id,
            "sender_id":        sender_id,
            "message":          message
        }
        if reply_to:
            body["reply_parent_message_id"] = reply_to

        return self._post("/chat/messages", body=body)
    
    def cmd_delete_message(self, broadcaster_id: str, moderator_id: str, message_id :str) -> None:
        """
        Deletes a chat message.
        
        Args:
            broadcaster_id: The ID of the channel
            moderator_id:   The ID of the bot (Mod)
            message_id:     The ID of the message to delete
        """
        self._delete("/moderation/chat", params={
            "broadcaster_id":   broadcaster_id,
            "moderator_id":     moderator_id,
            "message_id":       message_id
        })
    
    def cmd_ban_user(self, broadcaster_id: str, moderator_id: str, user_id: str, reason: str) -> None:
        """
        Permanently bans a user.
        """
        self._post("/moderation/bans", body={
            "broadcaster_id":   broadcaster_id,
            "moderator_id":     moderator_id,
            "data": {
                "user_id":          user_id,
                "reason":           reason
            }
        })
    
    def cmd_unban_user(self, broadcaster_id: str, moderator_id: str, user_id: str) -> None:
        """
        Unbans or removes a timeout from a user.
        """
        self._delete("/moderation/bans", params={
            "broadcaster_id":   broadcaster_id,
            "moderator_id":     moderator_id,
            "user_id":          user_id
        })
    
    def cmd_timeout_user(self, broadcaster_id: str, moderator_id: str, user_id: str, duration: int, reason: str = "") -> None:
        """
        Timeouts a user for a given duration in seconds.
        """
        self._post("/moderation/bans", body={
            "broadcaster_id":   broadcaster_id,
            "moderator_id":     moderator_id,
            "data": {
                "user_id":          user_id,
                "duration":         duration,
                "reason":           reason
            }
        })
        return
    
    def cmd_get_subscribers(self, broadcaster_id: str) -> list:
        """
        [REQUIRES YOU TO BE THE OWNER OF THE TARGETED CHANNEL]

        Fetches all subscribers for the given broadcaster.
        """
        subscribers = []
        cursor      = None

        while True:
            params = {
                "broadcaster_id": broadcaster_id,
                "first": 100
            }
            if cursor:
                params["after"] = cursor
            
            data = self._get("/subscriptions", params=params)
            subscribers.extend(data.get("data", []))
            cursor = data.get("pagination", {}).get("cursor")

            if not cursor:
                break
        
        return subscribers

# ---------------------------------------------------------------
# EventSub WebSocket Client
EVENTSUB_WS_URL = "wss://eventsub.wss.twitch.tv/ws"

class EventSubClient:
    """
    Manages a single EventSub WebSocket connection.
    """
    def __init__(self, auth: Auth, helix: HelixClient):
        self.auth                           = auth
        self.helix                          = helix
        self.session_id:     str | None     = None
        self._ws                            = None
        self._url                           = EVENTSUB_WS_URL
        self._running                       = False
        self._reconnect_url: str | None     = None
        
    #####
    async def connect(self):
        try:
            import websockets
        except ImportError:
            log_raise(
                "[EventSubClient.connect]",
                ImportError("websockets is not installed. Please create a virtual env and installed it."),
                reason="Import Error"
            )
        
        self._running = True
        await self._run_loop()

    async def _run_loop(self):
        import websockets

        url = self._reconnect_url or self._url
        self._reconnect_url = None

        log("[EventSubClient._run_loop]", f"Connecting to {url}...")

        try:
            async with websockets.connect(url) as ws:
                self._ws = ws
                log("[EventSubClient._run_loop]", "WebSocket connected.", reason="OK")

                # <Bug> Method below doesn't work so applying a different patch </Bug>
                # async for raw in ws:
                #     await self._handle_message(raw)

                #     if self._reconnect_url:
                #         break


                # <Fix> Attempt 1 for a solution </Fix>
                while True:
                    try:
                        raw = await ws.recv()
                    except ConnectionClosedOK:
                        break
                    except ConnectionClosedError as e:
                        log("[EventSubClient._run_loop]", str(e), reason="Error")
                        break

                    try:
                        await self._handle_message(raw)
                    except Exception as e:
                        log("[EventSubClient._run_loop]", str(e), reason="Error")

                    if self._reconnect_url:
                        break

        except Exception as e:
            log("[EventSubClient._run_loop]", str(e), reason="Error")
            
        if self._running and self._reconnect_url:
            log("[EventSubClient._run_loop]", "Reconnecting...", reason="Reconnect")
            await self._run_loop()
    

    async def _handle_message(self, raw: str):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError as e:
            log("[EventSubClient._handle_message]", f"Bad JSON: {e}", reason="Error")
            return
        
        msg_type = msg.get("metadata", {}).get("message_type", "")

        match msg_type:
            case "session_welcome":     await self._on_welcome(msg)
            case "notification":        await self._on_notification(msg)
            case "session_keepalive":   pass
            case "session_reconnect":   await self._handle_reconnect(msg)
            case "revocation":          await self._on_revocation(msg)
            case _:
                log("[EventSubClient._handle_message]", f"Unknown message type: {msg_type}", reason="Warning")
    

    #####
    async def _on_welcome(self, msg: dict):
        """
        Called when the WebSocket session is established.
        Override this (or subclass) to subscribe to your EventSub topics
        """
        session = msg["payload"]["session"]
        self.session_id = session["id"]
        keepalive = session.get("keepalive_timeout_seconds", "?")

        log("[EventSubClient._on_welcome]", f"Session ID: {self.session_id}  (keepalive: {keepalive}s)", reason="Welcome")
    
    async def _on_notification(self, msg: dict):
        """
        Called for every incoming EventSub event notification.
        Override this in your Bot subclass to handle events.
        """
        sub_type = msg["payload"]["subscription"]["type"]
        log("[EventSubClient._on_notification]", f"Notification: {sub_type}", reason="Event")
    
    async def _on_revocation(self, msg: dict):
        """Called when Twitch revokes a subscription (eg.)"""
        payload = msg["payload"]
        sub = payload.get("subscription", {})
        log(
            "[EventSubClient._on_revocation]",
            f"Subscription revoked: {sub.get('type')}  reason: {sub.get('status')}",
            reason="Revocation"
        )
    
    async def _handle_reconnect(self, msg: dict):
        """Stores the reconnect URL and lets the run loop re-establish the connection."""
        url = msg["payload"]["session"].get("reconnect_url")
        log("[EventSubClient._handle_reconnect]", f"Reconnect URL received: {url}", reason="Reconnect")
        self._reconnect_url = url

    #####

    async def close(self):
        """Shuts down the WebSocket connection."""
        self._running = False
        if self._ws:
            await self._ws.close()
            log("[EventSubClient.close]", "Connection closed.", reason="Shutdown")


# ---------------------------------------------------------------
# Bot
class Bot:
    """
    High-level entry point

    Required env vars:
        CLIENT_ID
        CLIENT_SECRET
        TWITCH_REFRESH_TOKEN
        BOTNAME
        CHANNEL

    BOTNAME -> The bot twitch name
    CHANNEL -> The target channel for the bot to go to

    Usage:
        class MyBot(Bot):
            async def _on_notification(self, msg: dict):
                ...

        bot = MyBot(env_path="path_to_env")
        bot.run()
    """
    def __init__(self, env_path: str = None):
        load_dotenv(env_path)

        self.auth   =   Auth(env_path=env_path)
        self.helix  =   HelixClient(self.auth)
        self.ws     =   EventSubClient(self.auth, self.helix)

        self.bot_name   = os.getenv("BOTNAME")
        self.channel    = os.getenv("CHANNEL")

        for key, val in {"BOTNAME": self.bot_name, "CHANNEL": self.channel}.items():
            if not val:
                log_raise("[Bot.__init__]", ValueError(f"Missing env var: {key}"), reason="ValueError")

        bot_user        = self.helix.get_user(login=self.bot_name)
        channel_user    = self.helix.get_user(login=self.channel)

        self.bot_user_id    = bot_user.get("id")
        self.broadcaster_id = channel_user.get("id")

        if not self.bot_user_id:
            log_raise(
                "[Bot.__init__]",
                ValueError(f"Could not resolve user ID for bot '{self.bot_name}'"), 
                reason="ValueError"
            )
        if not self.broadcaster_id:
            log_raise(
                "[Bot.__init__]",
                ValueError(f"Could not resolve user ID for channel '{self.channel}'"),
                reason="ValueError"
            )
        
        log(
            "[Bot.__init__]",
            f"Bot: {self.bot_name} ({self.bot_user_id}) -> Channel: {self.channel} ({self.broadcaster_id})"
        )

        _original_welcome = self.ws._on_welcome
        async def _welcome_and_subscribe(msg: dict):
            await _original_welcome(msg)
            await self._subscribe_default_topics()
        self.ws._on_welcome = _welcome_and_subscribe
        self.ws._on_notification = self._on_notification
    

    async def _subscribe_default_topics(self):
        """
        Subscribes to the default set of EventSub topics.
        Add or remove subscriptions here as you build out your bot.
        """
        topics = [
            ("channel.chat.message", "1", {
                "broadcaster_user_id" : self.broadcaster_id,
                "user_id" :             self.bot_user_id
            }),
        ]
        
        for sub_type, version, condition in topics:
            try:
                result = self.helix.subscribe_eventsub(
                    session_id=self.ws.session_id,
                    sub_type=sub_type,
                    version=version,
                    condition=condition
                )
                sub_id = result.get("data", [{}])[0].get("id", "?")
                log(
                    "[Bot._subscribe_default_topics]",
                    f"Subscribed: {sub_type}  (id: {sub_id})",
                    reason="OK"
                )
            except Exception as e:
                log(
                    "[Bot._subscribe_default_topics]",
                    str(e),
                    reason="Error"
                )
    
    def _resolve_user(self, login: str) -> tuple[str, str]:
        """
        Resolves a username to their (user_id, display_name)
        Raises ValueError if the user doesn't exist.
        """
        user = self.helix.get_user(login=login)
        if not user:
            raise ValueError(f"User '{login}' not found.")
        return user.get("id"), user.get("display_name")


    #####
    
    def run(self):
        """Starts the async event loop. Blocks until disconnected or interrupted."""
        log("[Bot.run]", f"{self.bot_name} starting up...")
        try:
            asyncio.run(self.ws.connect())
        except KeyboardInterrupt:
            log("[Bot.run]", "Keyboard interrupt, shutting down.", reason="Shutdown")