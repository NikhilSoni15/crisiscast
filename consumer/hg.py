from huggingface_hub import login, HfApi

# Log in to Hugging Face
login()  # Will prompt you to paste your HF token

# Upload model
api = HfApi()
api.upload_folder(
    folder_path="classification/models/mistral-lora-final",  # Make sure this path is correct
    repo_id="nikhilsoni700/mistral-lora-crisiscast",
    repo_type="model"
)
