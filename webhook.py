from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/webhook-receiver', methods=['POST'])
def webhook_receiver():
    data = request.json
    print(f"Received webhook data: {data}")
    return jsonify({"message": "Webhook received successfully"}), 200

if __name__ == '__main__':
    app.run(port=5003, debug=True)
