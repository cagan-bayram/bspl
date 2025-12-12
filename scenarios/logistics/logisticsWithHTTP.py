import argparse
import logging
import uuid
from bspl.adapter.core import Adapter
from bspl.adapter.http_adapter import HTTPEmitter, HTTPReceiver
from bspl.adapter.event import InitEvent
from bspl import load_file

logistics = load_file("logistics.bspl").export("Logistics")
from Logistics import Merchant, Wrapper, Labeler, Packer

base_urls = {
    "Merchant": "http://127.0.0.1:8000",
    "Wrapper": "http://127.0.0.1:8001",
    "Labeler": "http://127.0.0.1:8002",
    "Packer": "http://127.0.0.1:8003",
}

agents = {
    "Merchant": [("127.0.0.1", 8000)],
    "Wrapper": [("127.0.0.1", 8001)],
    "Labeler": [("127.0.0.1", 8002)],
    "Packer": [("127.0.0.1", 8003)],
}

systems = {
    "logistics": {
        "protocol": logistics,
        "roles": {
            Merchant: "Merchant",
            Wrapper: "Wrapper",
            Labeler: "Labeler",
            Packer: "Packer",
        },
    },
}


def build_adapter(role: str, initiate: bool = False) -> Adapter:
    adapter = Adapter(
        role,
        systems=systems,
        agents=agents,
        emitter=HTTPEmitter(base_urls=base_urls),
        receiver=HTTPReceiver(host="0.0.0.0", port=agents[role][0][1]),
    )

    log = logging.getLogger(f"logistics.{role}")
    log.setLevel(logging.INFO)

    # For the initiator role, register a decision handler that fires on InitEvent
    if initiate and role == "Merchant":
        request_label = logistics.messages["RequestLabel"]
        request_wrapping = logistics.messages["RequestWrapping"]

        @adapter.decision(event=InitEvent)
        async def send_initial(_enabled, _event):
            # First message has no preconditions; kick off protocol
            return request_label(orderID="order123", address="123 Main St")

        @adapter.reaction(request_label)
        async def after_label(msg):
            # Once RequestLabel is emitted/observed, send RequestWrapping using same orderID
            wrap = request_wrapping(
                orderID=msg["orderID"], itemID="item456", item="widget"
            )
            await adapter.send(wrap)

        # Log packed responses like the normal merchant
        packed = logistics.messages["Packed"]

        @adapter.reaction(packed)
        async def packed_handler(msg):
            log.info(
                f"Order {msg['orderID']} item {msg['itemID']} packed with status: {msg['status']}"
            )
            return msg

    if role == "Wrapper":
        request_wrapping = logistics.messages["RequestWrapping"]
        wrapped = logistics.messages["Wrapped"]

        @adapter.reaction(request_wrapping)
        async def wrap(msg):
            wrapping = "bubblewrap" if msg["item"] in ["plate", "glass"] else "paper"
            log.info(
                f"Order {msg['orderID']} item {msg['itemID']} ({msg['item']}) wrapped with {wrapping}"
            )
            await adapter.send(
                wrapped(
                    wrapping=wrapping,
                    **msg.payload,
                )
            )
            return msg

    if role == "Labeler":
        request_label = logistics.messages["RequestLabel"]
        labeled = logistics.messages["Labeled"]

        @adapter.reaction(request_label)
        async def label(msg):
            label_value = str(uuid.uuid4())
            log.info(f"Generated label {label_value} for order {msg['orderID']}")
            await adapter.send(labeled(label=label_value, **msg.payload))
            return msg

    if role == "Packer":
        packed = logistics.messages["Packed"]

        @adapter.enabled(packed)
        async def pack(msg):
            msg["status"] = "packed"
            log.info(
                f"Order {msg['orderID']} item {msg['itemID']} packed with wrapping {msg['wrapping']} and label {msg['label']}"
            )
            return msg

    return adapter


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one Logistics agent over HTTP")
    parser.add_argument(
        "role",
        choices=["Merchant", "Wrapper", "Labeler", "Packer"],
        help="Agent role to run",
    )
    parser.add_argument(
        "--initiate", action="store_true", help="Send initial message (for Merchant)"
    )
    args = parser.parse_args()

    adapter = build_adapter(args.role, initiate=args.initiate)
    adapter.start()  # blocks until shutdown
