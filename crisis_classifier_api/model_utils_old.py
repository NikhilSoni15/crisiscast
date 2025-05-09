from transformers import AutoTokenizer, AutoModelForCausalLM
import torch
import re

model_path = "C:/Users/omer/Downloads/crisiscast/classification/models/mistral-lora-final"

# Load tokenizer
tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)

# Set device
device = torch.device("cuda:1" if torch.cuda.device_count() > 1 else "cuda:0")

# Load model in half precision (fp16)
model = AutoModelForCausalLM.from_pretrained(
    model_path,
    torch_dtype=torch.float16,
    device_map={"": device},
    trust_remote_code=True
)


def strip_emojis(text):
    return re.sub(r'[^\w\s,.!?@:/\-]', '', text)

def classify_text(title: str) -> str:
    cleaned_title = title.strip().replace("\n", " ")
    prompt = (
        "<s>[INST] What type of crisis is described below? Choose one of: "
        "natural_disaster, terrorist_attack, cyberattack, pandemic, war, financial_crisis, "
        "civil_unrest, infrastructure_failure, environmental_crisis, crime, none\n\n"
        f"{cleaned_title} [/INST]"
    )
    inputs = tokenizer(prompt, return_tensors="pt").to(device)
    outputs = model.generate(**inputs, max_new_tokens=5)
    prediction = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return prediction.strip().split()[-1]

def classify_batch_texts(titles: list[str]) -> list[str]:
    results = []

    # Prepare prompts
    prompts = []
    for title in titles:
        # cleaned_title = title.strip().replace("\n", " ")
        cleaned_title = strip_emojis(title.strip().replace("\n", " "))
        prompt = (
            "<s>[INST] What type of crisis is described below? Choose one of: "
            "natural_disaster, terrorist_attack, cyberattack, pandemic, war, financial_crisis, "
            "civil_unrest, infrastructure_failure, environmental_crisis, crime, none\n\n"
            f"{cleaned_title} [/INST]"
        )
        prompts.append(prompt)

    try:
        inputs = tokenizer(prompts, return_tensors="pt", padding=True, truncation=True, max_length=512).to(device)
        outputs = model.generate(
            **inputs,
            max_new_tokens=5,
            pad_token_id=tokenizer.eos_token_id,
            eos_token_id=tokenizer.eos_token_id
        )

        for output in outputs:
            prediction = tokenizer.decode(output, skip_special_tokens=True)
            label = prediction.strip().split()[-1]
            results.append(label)

    except Exception as e:
        print(f"⚠️ Batch processing error: {e}")
        results = ["none"] * len(titles)
    torch.cuda.empty_cache()
    return results
