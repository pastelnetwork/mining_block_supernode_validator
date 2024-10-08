# Pastel Mining Block Supernode Validator API

## Overview
The Pastel Mining Block Supernode Validator API is designed to provide a robust and efficient way to sign and validate proposed mined blocks and mining shares on the Pastel Blockchain. This API is an essential component of the Pastel network, ensuring the integrity and reliability of mining operations.

## Features
- **Round-Robin Supernode Selection**: Implements a round-robin approach to select active supernodes for block signing, enhancing fairness and decentralization.
- **Payload Signing**: Allows signing of payloads with PastelIDs, with each active supernode having an equal opportunity to sign blocks over time.
- **Signature Validation**: Validates the signed payloads to ensure authenticity and consistency.
- **API Key Authentication**: Secures endpoints with API key authentication for enhanced security.
- **Database Integration**: Stores signed payloads along with their metadata in a database for auditing and tracking purposes.

## Getting Started

### Prerequisites
- Python 3.8 or higher
- FastAPI
- Uvicorn
- SQLAlchemy (AsyncIO version)
- PyYAML
- A PostgreSQL or SQLite database (configurable)

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/pastelnetwork/mining_block_supernode_validator.git
   ```
2. Navigate to the cloned directory:
   ```bash
   cd mining_block_supernode_validator
   ```
3. Set up the project:
   ```bash
    python3 -m venv venv
    source venv/bin/activate
    python3 -m pip install --upgrade pip
    python3 -m pip install wheel
    pip install -r requirements.txt
   ```

### Configuration
- Configure the `.env` file with the necessary environment variables such as `UVICORN_PORT` (default is `9997`) and `AUTH_TOKEN`.
- Update `pastelids_for_testnet_sns.yml` with the PastelIDs and other necessary details for the supernodes. This file should look something like this:
    ```yaml
    all:
    SuperNode01:
        col_address: tPX7gmMjErHz8rMySTn7J1xXiygsRS5jijX
        col_address_pkey: 123
        ind: 0
        other_address: tPWnxyRQ2DZw1TDx1RYGX1y9XpNB4EPDzdC
        other_address_pkey: 123
        pastelid: jXZ6VsP7LkNJE7oSrNRbvYfUHVLFySKeGyDUrTign84UURDohKDXcr49cRRG7fw8gjRxbtLL8ReGHgjfmv7z9y
        pkey: 123
        pwd: passphrase123
        txid: 76a9143e03ee6eb0ac37b9e313fade8932b9f9a69e70ce88ac
    SuperNode02:
        col_address: tPd5qgTjpdJQS48NTPmvG6qYwRch64RFxeF
        col_address_pkey: 123
        ind: 2
        other_address: tPm8AvT1uSfChTczUUxQarnwyKy74xzLopC
        other_address_pkey: 123
        pastelid: jXYs3T1PEe8mNdcLThm3TnVQvzhAxEKdi7gTG2x9EdSfqViwaF3E8T4utvv5LXB9JT7oD1roW4FZVvDp5eDXaM
        pkey: 123
        pwd: passphrase123
        txid: 76a9147f7b4be7dd0f12d11f3fe58aee4360a0b8bd75a088ac
    ```

### Running the API
- Start the API server using Uvicorn:
  ```bash
  uvicorn main:app --reload --host 0.0.0.0 --port 9997
  ```
- Access the Swagger UI for testing the API at `http://localhost:9997/`.

## API Endpoints
- `GET /list_pastelids`: Lists all the PastelIDs.
- `POST /sign_payload`: Accepts a payload, selects a supernode based on round-robin logic, and returns the signed data.

## Database Schema
- The API uses the `SignedPayload` model to store data in the database.

## Logging
- Detailed logging is implemented for tracking and debugging purposes.

## Contributing
Contributions to the Pastel Mining Block Supernode Validator API are welcome. Please read our [contributing guidelines](CONTRIBUTING.md) before submitting your pull requests.

## License
Distributed under the MIT License. See [LICENSE](LICENSE) for more information.

## Contact
- Project Link: [https://github.com/pastelnetwork/mining_block_supernode_validator](https://github.com/pastelnetwork/mining_block_supernode_validator)