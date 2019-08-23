from sys import argv
from argparse import ArgumentParser

from azure.servicebus import QueueClient, Message

service_bus_namespace = "servicebusns-5003"
access_key_name = "RootManageSharedAccessKey"
access_key = "ZZCtmtN7XUHmleZwENlBkkaI/B96E5ytNF7OPD35E40="
endpoint = "Endpoint=sb://{0}.servicebus.windows.net/;SharedAccessKeyName={1};SharedAccessKey={2}".format(service_bus_namespace, access_key_name, access_key)


def main():
    parser = ArgumentParser(description="An Azure ServiceBus Queue Consumer")
    parser.add_argument("queue_name",
                        help="The queue name to consume from")
    parser.add_argument("--dlq",
                        action="store_true",
                        help="Connect to the dead letter queue")
    args = parser.parse_args()

    queue_name = args.queue_name
    if (args.dlq):
        queue_name = queue_name + "/$deadletterqueue"

    # print("Endpoint: {0}".format(endpoint))
    # print("QueueName: {0}".format(queue_name))

    # Create the QueueClient
    queue_client = QueueClient.from_connection_string(endpoint, queue_name)

    # Receive the message from the queue
    with queue_client.get_receiver() as queue_receiver:
        messages = queue_receiver.fetch_next(timeout=3)
        while messages:
            for message in messages:
                print(message)
                message.complete()
            messages = queue_receiver.fetch_next(timeout=3)


if __name__ == '__main__':
    main()
