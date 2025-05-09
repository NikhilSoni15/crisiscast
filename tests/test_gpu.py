import torch
print("✅ CUDA Available:", torch.cuda.is_available())

from bitsandbytes import nn
print("✅ bitsandbytes loaded successfully")
