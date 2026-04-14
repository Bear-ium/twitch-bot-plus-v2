import os, socket, requests, threading, queue
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import Callable
from rich.console import Console

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
#   Networking
class IRCClient():
    def __init__(
            self,
            TOKEN: str,
            NICK: str,
            CHANNEL: str,
            SERVER: str = "irc.chat.twitch.tv",
            PORT: int = 6667
        ):
        """
        Creates an IRC Client config.
        Args:
            TOKEN (str):    The OAuth token (must start with 'oauth:')
            NICK (str):     The bot's Twitch username
            CHANNEL (str):  The channel to join (must start with '#')
            SERVER (str):   IRC server hostname
            PORT (int):     IRC port
        """

        if not TOKEN.startswith("oauth:"):
            log_raise("[IRCClient.__init__]", ValueError("OAuth token must start with 'oauth:'"), reason="ValueError")
        if not NICK:
            log_raise("[IRCClient.__init__]", ValueError("Nick cannot be empty."), reason="ValueError")
        if not CHANNEL.startswith("#"):
            CHANNEL = f"#{CHANNEL}"
        
        self.token          = TOKEN
        self.nickname       = NICK
        self.channel        = CHANNEL
        self.server         = SERVER
        self.port           = PORT
        self.irc            = None
        self.recv_buffer    = ""
    
    def connect(self):
        """
        Connects to the IRC Server and joins the specific channel.
        """

        self.irc = socket.socket()
        self.irc.connect((self.server, self.port))
        self.irc.settimeout(1)

        self.send_raw(f"PASS {self.token}")
        self.send_raw(f"NICK {self.nickname}")
        self.send_raw(f"JOIN {self.channel}")

    def send_raw(self, MESSAGE: str):
        """
        Sends a raw message to the IRC Server.

        Args:
            MESSAGE (str):  The raw IRC message to send
        """
        if not self.irc:
            log_raise("[IRCClient.send_raw]", ConnectionError("IRC Connection is not established."), reason="ConnectionError")
        
        self.irc.send((MESSAGE + "\r\n").encode("utf-8"))
        log("[IRCClient.send_raw]", {MESSAGE})
    
    def recv(self, BUFFER_SIZE: int = 2048):
        """
        Receives data from the IRC Server.
        
        Args:
            BUFFER_SIZE (int): The buffer size for receiving data
        """
        if not self.irc:
            log_raise("[IRCClient.recv]", ConnectionError("IRC Connection is not established."), reason="ConnectionError")
        
        while '\r\n' not in self.recv_buffer:
            try:
                CHUNK = self.irc.recv(BUFFER_SIZE).decode("utf-8")
                if not CHUNK:
                    log_raise("[IRCClient.recv]", ConnectionError("Disconnected from server."), reason="ConnectionError")
                self.recv_buffer += CHUNK
            except socket.timeout:
                raise
        
        line, self.recv_buffer = self.recv_buffer.split('\r\n', 1)
        return line

    def close(self):
        """
        Closes the IRC Connection.
        """
        if self.irc:
            self.irc.close()
            self.irc = None

# ---------------------------------------------------------------
#   Auth
class Auth:
    def __init__(self, ENV_PATH = None):
        if ENV_PATH is None:
            log_raise("[Auth.__init__]", ValueError("ENV_PATH is required but was not provided."), reason="ValueError")
        
        load_dotenv(ENV_PATH)
        self.env_path = ENV_PATH

        self.CLIENT_ID      = os.getenv('CLIENT_ID')
        self.CLIENT_SECRET  = os.getenv('CLIENT_SECRET')
        self.REFRESH_TOKEN  = os.getenv('TWITCH_REFRESH_TOKEN')
        self.OAuth_TOKEN    = None

        self.refresh_access_token()

    def refresh_access_token(self):
        payload = {
            'client_id':        self.CLIENT_ID,
            'client_secret':    self.CLIENT_SECRET,
            'refresh_token':    self.REFRESH_TOKEN,
            'grant_type':       'refresh_token'
        }

        response = requests.post('https://id.twitch.tv/oauth2/token', data=payload)
        if response.status_code != 200:
            log_raise("[Auth.refresh_access_token]", Exception(f"Failed to refresh token: {response.text}"), reason="Exception")
        
        data = response.json()
        self.OAuth_TOKEN = f"oauth:{data['access_token']}"
        self.REFRESH_TOKEN = data['refresh_token']

        self._update_env_file()
        self._update_runtime_env()

    def _update_env_file(self):
        with open(self.env_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        
        with open(self.env_path, 'w', encoding='utf-8') as file:
            for line in lines:
                if line.startswith('TWITCH_OAUTH='):
                    file.write(f'TWITCH_OAUTH={self.OAuth_TOKEN}\n')
                elif line.startswith('TWITCH_REFRESH_TOKEN='):
                    file.write(f'TWITCH_REFRESH_TOKEN={self.REFRESH_TOKEN}\n')
                else:
                    file.write(line)
    
    def _update_runtime_env(self):
        if self.OAuth_TOKEN is not None:
            os.environ['TWITCH_OAUTH'] = self.OAuth_TOKEN
        if self.REFRESH_TOKEN is not None:
            os.environ['TWITCH_REFRESH_TOKEN'] = self.REFRESH_TOKEN

    def get_oauth_token(self):
        return self.OAuth_TOKEN

# ---------------------------------------------------------------
# Twitch Returns
def TW_Send(irc: IRCClient, CHANNEL: str, MESSAGE: str):
    """
    Sends a message to the specified IRC Channel.

    Args:
        irc (socket):   The active IRC Socket connection
        CHANNEL (str):  The Twitch channel to send the message to
        MESSAGE (str):  The message content
    """
    if not CHANNEL.startswith("#"):
        CHANNEL = f"#{CHANNEL}"
    irc.send_raw(f"PRIVMSG {CHANNEL} :{MESSAGE}")

def TW_GetUser(raw: str) -> str:
    """
    Extracts the sender's username from the raw IRC message.

    Args:
        raw (split): Raw IRC PRIVMSG string
    """
    return raw.split("!",1)[0][1:]

# ---------------------------------------------------------------
#   Dataclasses
@dataclass
class MessageEvent:
    irc:        IRCClient
    channel:    str
    user:       str
    text:       str
    bits:       int = 0

    def send(self, message: str):
        TW_Send(self.irc, self.channel, message)

    def reply(self, message: str):
        TW_Send(self.irc, self.channel, f"@{self.user} {message}")

@dataclass
class CommandEvent(MessageEvent):
    command: str        = ""
    args:    list[str]  = None

    def __post_init__(self):
        if self.args is None:
            self.args = []

@dataclass
class SubEvent:
    irc:        IRCClient
    channel:    str
    user:       str
    sub_plan:   str
    months:     int = 0
    message:    str = ""
    is_gift:    bool = False
    gifted_to:  str = ""

    def send(self, message: str):
        TW_Send(self.irc, self.channel, message)

@dataclass
class RaidEvent:
    irc:            IRCClient
    channel:        str
    raider:         str
    viewer_count:   int = 0

    def send(self, message: str):
        TW_Send(self.irc, self.channel, message)

# ---------------------------------------------------------------
# Core
class Bot:
    def __init__(self, handle: str = "!", env_path: str = None):
        load_dotenv(env_path)
        auth = Auth(ENV_PATH=env_path)

        self.name       = os.getenv("BOTNAME")
        self.token      = auth.get_oauth_token()
        self.channel    = os.getenv("CHANNEL")
        self.handle     = handle

        missing = [k for k, v in {
            "BOTNAME":  self.name,
            "TOKEN":    self.token,
            "CHANNEL":  self.channel
        }.items() if not v]
        if missing:
            log_raise("[Bot.__init__]", ValueError(f"Missing env vars: {', '.join(missing)}"), reason="ValueError")
        
        self._listeners:    dict[str, list[Callable]]   = {}
        self._commands:     dict[str, Callable]         = {}
        self._shutdown  = threading.Event()
        self._queue     = queue.Queue()

        self.irc = IRCClient(self.token, self.name, self.channel)
        self.irc.connect()
    
    # Decorators
    def on(self, event: str):
        """
        Register a listener for a Twitch event,
        Valid events: "message", "command", "bits", "sub", "raid"

        Usage:
            @bot.on("sub")
            def on_sub(event: SubEvent):
                ...
        """
        def decorator(fn: Callable):
            self._listeners.setdefault(event, []).append(fn)
            return fn
        return decorator
    
    def command(self, name: str):
        """
        Register a command handler. The handle prefix is stripped automatically.

        Usage:
            @bot.command("Hello")
            def hello(event: CommandEvent):
                ...
        """
        def decorator(fn: Callable):
            self._commands[name.lower()] = fn
            return fn
        return decorator
    
    # Internal
    def _dispatch(self, event: str, data):
        for fn in self._listeners.get(event, []):
            try:
                fn(data)
            except Exception as e:
                log(f"[Bot._dispatch:{event}]", str(e), reason="Error")
    
    # IRC tag parsing
    def _parse_tags(self, raw: str) -> tuple[dict, str]:
        """
        Strips the @tag prefix and returns (tags_dict, remaining_line).
        """
        if not raw.startswith("@"):
            return {}, raw
        tag_str, reset = raw[1:].split(" ", 1)
        tags = {
            k: v
            for part in tag_str.split(";")
            if "=" in part
            for k, v in [part.split("=", 1)]
        }
        return tags, reset
    
    # Message routing
    def handle_privmsg(self, raw: str, tags: dict):
        parts = raw.split(":", 2)
        if len(parts) < 3:
            return
        
        user    = TW_GetUser(raw)
        text    = parts[2].strip()
        bits    = int(tags.get("bits", 0))
        words   = text.split()

        if not words:
            return
        
        msg = MessageEvent(
            irc     = self.irc,
            channel = self.channel,
            user    = user,
            text    = text, 
            bits    = bits
        )

        self._dispatch("message", msg)

        if bits:
            self._dispatch("bits", msg)
        
        if words[0].startswith(self.handle):
            cmd_name = words[0][len(self.handle):].lower()
            cmd = CommandEvent(
                irc     = self.irc,
                channel = self.channel,
                user    = user,
                text    = text, 
                bits    = bits,
                command = cmd_name,
                args    = words[1:]
            )
            self._dispatch("command", cmd)
            if cmd_name in self._commands:
                try:
                    self._commands[cmd_name](cmd)
                except Exception as e:
                    log(f"[Bot.command:{cmd_name}]", str(e), reason="Error")

    def _handle_usernotice(self, raw: str, tags: dict):
        msg_id = tags.get("msg-id", "")
        
        if msg_id in ("sub", "resub", "subgift", "anonsubgift"):
            parts = raw.split(":", 2)
            self._dispatch("sub", SubEvent(
                irc         = self.irc,
                channel     = self.channel,
                user        = tags.get("display-name", ""),
                sub_plan    = tags.get("msg-param-sub-plan", ""),
                months      = int(tags.get("msg-param-cumulative-months"), 0),
                message     = parts[2].strip() if len(parts) >= 3 else "",
                is_gift     = "gift" in msg_id,
                gifted_to   = tags.get("msg-param-recipient-display-name", "")
            ))
        elif msg_id == "raid":
            self._dispatch("Raid", RaidEvent(
                irc             = self.irc,
                channel         = self.channel,
                raider          = tags.get("display-name", ""),
                viewer_count    = int(tags.get("msg-param-viewerCount", 0))
            ))
    
    def _route(self, raw: str):
        tags, msg = self._parse_tags(raw)
        if      "PRIVMSG"       in msg: self._queue.put(lambda m=msg, t=tags: self.handle_privmsg(m,t)) 
        elif    "USERNOTICE"    in msg: self._queue.put(lambda m=msg, t=tags: self._handle_usernotice(m,t))
    
    # Worker
    def _worker(self):
        while True:
            task = self._queue.get()
            if task is None:
                self._queue.task_done()
                break
            try:
                task()
            except Exception as e:
                log("[Bot._worker]", str(e), reason="Error")
            finally:
                self._queue.task_done()
    
    def run(self):
        self.irc.send_raw("CAP REQ :twitch.tv/tags twitch.tv/commands")

        threading.Thread(target=self._worker, daemon=True).start()
        log("[Bot.run]", f"{self.name} is live.")

        try:
            while not self._shutdown.is_set():
                try:
                    raw = self.irc.recv()
                except socket.timeout:
                    continue
                except Exception as e:
                    log("[Bot.run]", str(e), reason="Error")
                    continue

                if raw.startswith("PING"):
                    self.irc.send_raw("PONG :tmi.twitch.tv")
                else:
                    self._route(raw)
        except KeyboardInterrupt:
            log("[Bot.run]", "Keyboard interrupt received.", reason="Shutdown")
        finally:
            log("[Bot.run]", f"Shutting down {self.name}...", reason="Shutdown")
            self._queue.put(None)
            self._queue.join()
            self.irc.close()
    
    def stop(self):
        self._shutdown.set()