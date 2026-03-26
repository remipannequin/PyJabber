import logging
import asyncio
import ssl

from slixmpp import ComponentXMPP, JID, ClientXMPP


class EchoClient(ClientXMPP):
    def __init__(self, jid, password):
        super().__init__(jid, password)

        # activer l'inscription
        self.register_plugin("xep_0077")  # In-band registration
        
        # TLS configuration
        self.use_tls = True
        self.use_ssl = False  # False = STARTTLS (standard XMPP)
        self.ssl_context = ssl.create_default_context()

        # pour test local uniquement :
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.add_event_handler("session_start", self.start)
        self.add_event_handler("message", self.on_message)
        self.add_event_handler("register", self.on_register)

    async def start(self, event):
        print("[+] Session started")
        self.send_presence()
        await self.get_roster()

    def on_message(self, msg):
        if msg["type"] in ("chat", "normal"):
            print(f"[>] Received: {msg['body']}")
            msg.reply(f"Echo: {msg['body']}").send()

    async def on_register(self, iq):
        print("[*] Registering account...")

        resp = self.Iq()
        resp["type"] = "set"
        resp["register"]["username"] = self.boundjid.user
        resp["register"]["password"] = self.password

        try:
            await resp.send()
            print("[+] Registration successful")
        except Exception as e:
            print("[-] Registration failed:", e)
            self.disconnect()


class TestComponent(ComponentXMPP):

    def __init__(self, jid, secret, host, port):
        super().__init__(jid, secret, host, port)
        
        # Register event handlers
        self.add_event_handler("session_start", self.on_session_start)
        self.add_event_handler("connected", self.on_connected)
        self.add_event_handler("disconnected", self.on_disconnected)
        self.add_event_handler("failed_auth", self.on_failed_auth)

    def on_connected(self, event):
        print("[+] Connected to server")

    def on_session_start(self, event):
        print("[+] Session started (authenticated)")
        print("[+] Component is ready")
        self.loop.create_task(self.send_periodic_messages())

    def on_disconnected(self, event):
        print("[-] Disconnected from server")

    def on_failed_auth(self, event):
        print("[-] Authentication failed")

    async def send_periodic_messages(self):
        counter = 0
        while True:
            counter += 1
            await asyncio.sleep(5)  # toutes les 5 secondes
            print(f"[*] Sending message #{counter}")

            self.send_message(
                mto=JID("user@localhost"),
                mfrom=JID("agent@sub.localhost"),
                mbody=f"Test message {counter}",
                mtype="chat",
            )

            

async def start_component():
    

    jid = "sub.localhost"
    host = "127.0.0.1"
    port = 5347
    secret = "test_secret"
    
    xmpp = TestComponent(jid, secret, host, port)

    print("[*] Connecting...")
    if xmpp.connect():
        await xmpp.wait_until("session_start")
        print("[*] Processing events...")
        await asyncio.sleep(60)
        print("[*] Done, stopping")
        xmpp.disconnect()
    else:
        print("[-] Unable to connect")


async def start_client():
    xmpp = EchoClient("user@localhost", "password")

    print("[*] Connecting...")
    await xmpp.connect()

    await xmpp.wait_until("session_start", timeout=10)

    print("[*] Ready, waiting for messages...")
    await xmpp.disconnected


async def main():
    task1 = asyncio.create_task(start_component())
    task2 = asyncio.create_task(start_client())

    await task1
    await task2


if __name__ == "__main__":
    #logging.basicConfig(level=logging.DEBUG)

    asyncio.run(main())