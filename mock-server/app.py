import json
import os
from flask import Flask, jsonify, request, abort

app = Flask(__name__)

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "customers.json")


def load_customers():
    with open(DATA_FILE, "r") as f:
        return json.load(f)


@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "service": "mock-server"}), 200


@app.route("/api/customers", methods=["GET"])
def get_customers():
    customers = load_customers()

    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 10))
    except ValueError:
        return jsonify({"error": "page and limit must be integers"}), 400

    if page < 1 or limit < 1:
        return jsonify({"error": "page and limit must be positive integers"}), 400

    total = len(customers)
    start = (page - 1) * limit
    end = start + limit
    paginated = customers[start:end]

    return jsonify({
        "data": paginated,
        "total": total,
        "page": page,
        "limit": limit
    }), 200


@app.route("/api/customers/<customer_id>", methods=["GET"])
def get_customer(customer_id):
    customers = load_customers()
    customer = next((c for c in customers if c["customer_id"] == customer_id), None)

    if customer is None:
        abort(404)

    return jsonify({"data": customer}), 200


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Customer not found"}), 404


@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
