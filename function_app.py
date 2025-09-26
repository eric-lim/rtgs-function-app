import logging
import json
import os
import azure.functions as func
from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import DefaultAzureCredential

# Get ADT instance URL from an environment variable
adt_instance_url = os.environ.get("ADT_INSTANCE_URL")

# Authenticate with Azure
credential = DefaultAzureCredential()
client = DigitalTwinsClient(adt_instance_url, credential)

app = func.FunctionApp()

# The decorator connects the function to the Event Hub
# Cardinality is set to ONE to process one message at a time
@app.event_hub_message_trigger(arg_name="event",
                               event_hub_name="rtgs-transactions",
                               connection="EventHubConnectionString",
                               cardinality=func.Cardinality.ONE)
def ProcessRTGSTransaction(event: func.EventHubEvent):
    try:
        # Decode the message body from the event
        message_body = event.get_body().decode('utf-8')
        logging.info(f'Event Body: {message_body}')

        # Convert the JSON message string into a Python dictionary
        transaction = json.loads(message_body)

        sender_id = transaction.get("senderId")
        receiver_id = transaction.get("receiverId")
        amount = transaction.get("amount")
        txn_id = transaction.get("transactionId")

        # Basic validation
        if not all([sender_id, receiver_id, amount, txn_id]):
            logging.warning("Invalid transaction data received.")
            return

        # 1. Get the sender and receiver digital twins from ADT
        sender_twin = client.get_digital_twin(sender_id)
        receiver_twin = client.get_digital_twin(receiver_id)

        sender_balance = sender_twin['liquidityBalance']

        # 2. Perform the liquidity check
        if sender_balance >= amount:
            # Sufficient funds: Prepare updates for both twins
            new_sender_balance = sender_balance - amount
            receiver_balance = receiver_twin['liquidityBalance']
            new_receiver_balance = receiver_balance + amount

            # Create and send the update patch for the sender
            sender_patch = [{"op": "replace", "path": "/liquidityBalance", "value": new_sender_balance}]
            client.update_digital_twin(sender_id, sender_patch)

            # Create and send the update patch for the receiver
            receiver_patch = [{"op": "replace", "path": "/liquidityBalance", "value": new_receiver_balance}]
            client.update_digital_twin(receiver_id, receiver_patch)

            logging.info(f"SUCCESS: Transaction {txn_id} settled. Sender {sender_id} new balance: {new_sender_balance}, Receiver {receiver_id} new balance: {new_receiver_balance}")
        else:
            # Insufficient funds
            logging.warning(f"FAILURE: Insufficient funds for Transaction {txn_id}. Sender {sender_id} has {sender_balance}, but needs {amount}.")

    except Exception as e:
        logging.error(f'Error processing event: {e}')