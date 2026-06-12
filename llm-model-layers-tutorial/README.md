How an LLM Works, From the Inside
---

A hands-on Jupyter tutorial that demystifies transformer language models by building a tiny one from scratch in PyTorch and training it on the real [`tweet_eval`](https://huggingface.co/datasets/cardiffnlp/tweet_eval) sentiment dataset (3-class: negative / neutral / positive). It walks through every piece of jargon — tokenization, embeddings ("vectoring"), positional encoding, self-attention, multi-head attention, stacked layers, parameters, and logits — pausing at each step to inspect the actual tensors and reconcile the model's parameter count by hand. It ends with a short training loop and a held-out test evaluation (accuracy, precision/recall/F1, confusion matrix).

```bash
# Open in Colab:
# https://colab.research.google.com/github/kolodkin/samples/blob/main/llm-model-layers-tutorial/notebook.ipynb

# Or run locally:
jupyter lab llm-model-layers-tutorial/notebook.ipynb
```
