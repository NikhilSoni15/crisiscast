from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

# Use the original Mistral-7B-Instruct model hosted on Hugging Face
model_id = "mistralai/Mistral-7B-Instruct-v0.1"

# Load tokenizer and model
print("🔄 Loading tokenizer and model...")
tokenizer = AutoTokenizer.from_pretrained(model_id)
model = AutoModelForCausalLM.from_pretrained(
    model_id,
    torch_dtype=torch.float16,
    device_map="auto"  # Automatically picks available GPU
)

# Crisis labels (used only for prompt clarity, not matching)
crisis_labels = [
    "natural_disaster", "terrorist_attack", "cyberattack", "pandemic", "war",
    "financial_crisis", "civil_unrest", "infrastructure_failure", "environmental_crisis", "crime", "none"
]

def classify_text(text: str) -> str:
    prompt = (
        "<s>[INST] What type of crisis is described below? Choose one of: "
        + ", ".join(crisis_labels)
        + "\n\n" + text.strip().replace("\n", " ") + " [/INST]"
    )
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    outputs = model.generate(**inputs, max_new_tokens=10)
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return response.split("[/INST]")[-1].strip().lower().split()[0].strip(",.:;\n")

def classify_batch_texts(texts: list[str]) -> list[str]:
    results = []
    prompts = []
    for text in texts:
        cleaned_text = text.strip().replace("\n", " ")
        prompt = (
            "<s>[INST] What type of crisis is described below? Choose one of: "
            + ", ".join(crisis_labels)
            + "\n\n" + cleaned_text + " [/INST]"
        )
        prompts.append(prompt)

    try:
        inputs = tokenizer(prompts, return_tensors="pt", padding=True, truncation=True).to(model.device)
        outputs = model.generate(**inputs, max_new_tokens=10)
        for output in outputs:
            decoded = tokenizer.decode(output, skip_special_tokens=True)
            answer = decoded.split("[/INST]")[-1].strip().lower().split()[0].strip(",.:;\n")
            print(answer)
            results.append(answer)
    except Exception as e:
        print(f"⚠️ Batch classification error: {e}")
        results = ["none"] * len(texts)

    return results