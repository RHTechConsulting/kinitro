---
section: 'Get Started'
---

# Miner
### Setting up environment variables
Copy-paste the `.env.miner.example` file to `.env` and fill in the required environment variables:
```bash
cp .env.miner.example .env
```

### Configuration
Copy the example configuration file, and edit it to include parameters like your hugging face submission repo, bittensor wallet, etc.
```bash
cp configs/miner.toml.example miner.toml
```
Edit `configs/miner.toml` to set your desired parameters.

### Uploading your agent
To upload your agent to the Kinitro platform, use the following command:
```bash
python -m miner upload --config miner.toml
```
This command will package your agent, upload it to the specified huggingface repository.

### Committing submission info to the blockchain
After uploading your agent, you need to commit the submission information to the Bittensor blockchain. Use the following command:
```bash
python -m miner commit --config miner.toml
```
This command will create a new submission on the blockchain, linking to the uploaded agent.