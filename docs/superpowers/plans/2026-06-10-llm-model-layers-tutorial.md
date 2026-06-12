# LLM Model Layers Tutorial — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-contained, Colab-runnable Jupyter notebook that teaches transformer internals (parameters, layers, embeddings, attention) by constructing a tiny transformer classifier from scratch in PyTorch and training it on real `tweet_eval` sentiment data.

**Architecture:** A single `notebook.ipynb` of alternating markdown (explanation) and code (inspect/build) cells, walking from raw text → tokens → embeddings → attention → stacked layers → classification head → training → evaluation. The model is a small `nn.Module` with deliberately tiny dimensions so its parameter count reconciles by hand. Verification is headless notebook execution with `assert` statements embedded in the cells themselves (the asserts are also pedagogy — e.g. "our hand-computed parameter count equals the real total").

**Tech Stack:** Python, PyTorch, HuggingFace `datasets`, `scikit-learn` (metrics), `matplotlib` (loss curve). Jupyter via `nbconvert` for headless execution.

---

## Reference: design spec

Full design at `docs/superpowers/specs/2026-06-10-llm-model-layers-tutorial-design.md`. Key locked decisions:
- Task: 3-class tweet sentiment (`tweet_eval`, `sentiment` subset). Labels `0=neg, 1=neutral, 2=pos`.
- Use the dataset's provided train/validation/test splits; subsample for CPU speed; build vocab on train only.
- Word-level tokenization, `<pad>`/`<unk>` tokens, fixed max length.
- Model: token embedding + learned positional embedding → N transformer blocks (multi-head self-attention + MLP, residual + LayerNorm) → mean-pool over non-pad tokens → linear head → 3 logits.
- Tiny dims (e.g. `d_model=64`, `n_heads=2`, `n_layers=2`, `block_size=32`).
- Metrics: train+val loss curve; test accuracy vs majority baseline, macro P/R/F1, 3×3 confusion matrix.
- Intuition + light math; print shapes everywhere.

## File structure

```
llm-model-layers-tutorial/
├── notebook.ipynb   # the tutorial (built incrementally across tasks)
└── README.md        # setext title, one paragraph, run command (Colab + local)
```

Plus this plan + the existing spec under `docs/superpowers/`.

## Verification environment (do this ONCE before Task 2)

`torch`/`datasets`/`jupyter` are NOT in the repo's default env. Create an isolated venv for executing the notebook during development:

```bash
cd /home/user/samples/llm-model-layers-tutorial
uv venv .venv-tutorial --python 3.11
uv pip install --python .venv-tutorial torch --index-url https://download.pytorch.org/whl/cpu
uv pip install --python .venv-tutorial datasets scikit-learn matplotlib jupyter nbconvert ipykernel
```

Headless execution command used throughout (writes executed copy to /tmp so the committed notebook stays output-light until the final task):

```bash
cd /home/user/samples/llm-model-layers-tutorial
.venv-tutorial/bin/jupyter nbconvert --to notebook --execute notebook.ipynb \
  --output /tmp/nb_executed.ipynb --ExecutePreprocessor.timeout=300
```

A clean exit (return code 0) means every cell ran and every embedded `assert` passed. The `.venv-tutorial/` dir must be gitignored (Task 1 handles this).

**Network contingency:** if the sandbox blocks the HuggingFace download, the dataset cell will fail. In that case, verify model/training logic with the offline fallback described in Task 7's notes (synthetic in-memory batch), and flag in the final report that full end-to-end execution needs a networked run (e.g. Colab).

---

## Task 1: Scaffold project (directory, README, notebook skeleton, gitignore)

**Files:**
- Create: `llm-model-layers-tutorial/README.md`
- Create: `llm-model-layers-tutorial/notebook.ipynb`
- Modify: `.gitignore`

- [ ] **Step 1: Create the project directory and an empty notebook**

```bash
mkdir -p /home/user/samples/llm-model-layers-tutorial
```

Create `llm-model-layers-tutorial/notebook.ipynb` as a minimal valid notebook (nbformat 4) with a single markdown title cell:

```json
{
 "cells": [
  {"cell_type": "markdown", "metadata": {}, "source": ["# How an LLM Works, From the Inside\n", "\n", "A hands-on tour of a transformer's guts — built tiny, from scratch, and trained on real data."]}
 ],
 "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}, "language_info": {"name": "python"}},
 "nbformat": 4,
 "nbformat_minor": 5
}
```

- [ ] **Step 2: Write the README** (`llm-model-layers-tutorial/README.md`)

```markdown
How an LLM Works, From the Inside
---

A hands-on Jupyter tutorial that demystifies transformer language models by building a tiny one from scratch in PyTorch and training it on the real [`tweet_eval`](https://huggingface.co/datasets/tweet_eval) sentiment dataset (3-class: negative / neutral / positive). It walks through every piece of jargon — tokenization, embeddings ("vectoring"), positional encoding, self-attention, multi-head attention, stacked layers, parameters, and logits — pausing at each step to inspect the actual tensors and reconcile the model's parameter count by hand. It ends with a short training loop and a held-out test evaluation (accuracy, precision/recall/F1, confusion matrix).

\```bash
# Open in Colab:
# https://colab.research.google.com/github/kolodkin/samples/blob/main/llm-model-layers-tutorial/notebook.ipynb

# Or run locally:
jupyter lab llm-model-layers-tutorial/notebook.ipynb
\```
```

(Replace `\``` with real triple backticks when writing the file.)

- [ ] **Step 3: Gitignore the dev venv**

Add to `.gitignore`:

```
.venv-tutorial/
```

- [ ] **Step 4: Verify the notebook is valid JSON / nbformat**

```bash
cd /home/user/samples
python3 -c "import json; json.load(open('llm-model-layers-tutorial/notebook.ipynb')); print('valid notebook')"
```

Expected: `valid notebook`

- [ ] **Step 5: Commit**

```bash
git add llm-model-layers-tutorial/ .gitignore
git commit -m "Scaffold LLM model layers tutorial (README + notebook skeleton)"
```

---

## Task 2: Intro + config cells, set up verification env

**Files:**
- Modify: `llm-model-layers-tutorial/notebook.ipynb`

- [ ] **Step 1: Build the verification venv** (the one-time setup above). Confirm:

```bash
cd /home/user/samples/llm-model-layers-tutorial
.venv-tutorial/bin/python -c "import torch, datasets, sklearn, matplotlib; print('env ok')"
```

Expected: `env ok` (if torch install is slow, that's fine; CPU wheel only).

- [ ] **Step 2: Add an intro markdown cell** (after the title) explaining the mental model:

> Content: "A neural network is just a big pile of numbers — its **parameters** — organised into a sequence of functions called **layers**. 'Training' means nudging those numbers until the layers, applied in order, turn an input into a useful output. This notebook builds the smallest honest version of a modern language model — a **transformer** — so you can see every one of those numbers and what it does. We'll classify the sentiment of real tweets (negative / neutral / positive)." Keep it ~120 words, intuition-first.

- [ ] **Step 3: Add an install code cell** (guarded; no-op on Colab):

```python
# On Colab these are mostly preinstalled; this is a quiet guard for local runs.
import importlib, subprocess, sys
for pkg in ["torch", "datasets", "scikit-learn", "matplotlib"]:
    name = "sklearn" if pkg == "scikit-learn" else pkg
    if importlib.util.find_spec(name) is None:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg], check=True)
print("dependencies ready")
```

- [ ] **Step 4: Add an imports + config code cell**:

```python
import random, re
from collections import Counter
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
import matplotlib.pyplot as plt

SEED = 0
random.seed(SEED); np.random.seed(SEED); torch.manual_seed(SEED)

# --- Hyperparameters (all tunables live here) ---
BLOCK_SIZE   = 32     # max tokens per tweet
D_MODEL      = 64     # embedding / hidden width
N_HEADS      = 2      # attention heads per block
N_LAYERS     = 2      # stacked transformer blocks
D_FF         = 4 * D_MODEL  # feed-forward inner width
DROPOUT      = 0.1
N_CLASSES    = 3      # negative / neutral / positive

# --- Training / data budget (kept small so it runs on a CPU in ~1-2 min) ---
TRAIN_SUBSET = 4000
EVAL_SUBSET  = 1000
BATCH_SIZE   = 64
EPOCHS       = 8
LR           = 3e-3

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
print("device:", DEVICE)
```

- [ ] **Step 5: Verify execution so far**

```bash
cd /home/user/samples/llm-model-layers-tutorial
.venv-tutorial/bin/jupyter nbconvert --to notebook --execute notebook.ipynb \
  --output /tmp/nb_executed.ipynb --ExecutePreprocessor.timeout=300
echo "exit=$?"
```

Expected: `exit=0`, output shows `device: cpu`.

- [ ] **Step 6: Commit**

```bash
git add llm-model-layers-tutorial/notebook.ipynb
git commit -m "Add intro, install guard, and config cells to tutorial"
```

---

## Task 3: Data loading, splits, and the majority baseline

**Files:**
- Modify: `llm-model-layers-tutorial/notebook.ipynb`

- [ ] **Step 1: Add a markdown cell** introducing the data: explain `tweet_eval/sentiment`, the 3 labels, and that the dataset *ships its own* train/validation/test splits — and what each split is for (train = learn; validation = watch for overfitting / decide when to stop; test = one final honest score you only look at once).

- [ ] **Step 2: Add a code cell** that loads and subsamples:

```python
from datasets import load_dataset

raw = load_dataset("tweet_eval", "sentiment")
LABELS = ["negative", "neutral", "positive"]

def take(split, n):
    ds = raw[split].shuffle(seed=SEED).select(range(min(n, len(raw[split]))))
    return list(ds["text"]), list(ds["label"])

train_texts, train_labels = take("train", TRAIN_SUBSET)
val_texts,   val_labels   = take("validation", EVAL_SUBSET)
test_texts,  test_labels  = take("test", EVAL_SUBSET)

print(f"train={len(train_texts)}  val={len(val_texts)}  test={len(test_texts)}")
for t, y in list(zip(train_texts, train_labels))[:3]:
    print(f"[{LABELS[y]}] {t[:80]}")
```

- [ ] **Step 3: Add a code cell** showing class balance + the majority-class baseline (a key honesty check for imbalanced data):

```python
counts = Counter(train_labels)
print("train class counts:", {LABELS[k]: counts[k] for k in range(N_CLASSES)})
majority_class = counts.most_common(1)[0][0]
baseline_acc = np.mean(np.array(test_labels) == majority_class)
print(f"majority class = {LABELS[majority_class]!r}; "
      f"always-guess-majority test accuracy = {baseline_acc:.3f}")
assert len(train_texts) > 0 and len(test_texts) > 0
```

Add a one-line markdown note: "Our model has to *beat* this baseline to have learned anything."

- [ ] **Step 4: Verify execution**

```bash
cd /home/user/samples/llm-model-layers-tutorial
.venv-tutorial/bin/jupyter nbconvert --to notebook --execute notebook.ipynb \
  --output /tmp/nb_executed.ipynb --ExecutePreprocessor.timeout=300
echo "exit=$?"
```

Expected: `exit=0`; prints split sizes, sample tweets, class counts, baseline accuracy.
(If this fails with a network error, see the Network contingency note above.)

- [ ] **Step 5: Commit**

```bash
git add llm-model-layers-tutorial/notebook.ipynb
git commit -m "Add data loading, splits, and majority baseline cells"
```

---

## Task 4: Tokenization — text to integers (vocab fit on train)

**Files:**
- Modify: `llm-model-layers-tutorial/notebook.ipynb`

- [ ] **Step 1: Add a markdown cell**: "Symbols → integers." Explain word-level tokenization, why we lowercase + split on a simple regex, why `<pad>` (to make tweets equal length) and `<unk>` (for words unseen in training) exist, and that **the vocabulary is built from the training split only** — peeking at val/test to build vocab would be cheating.

- [ ] **Step 2: Add a code cell** building the tokenizer:

```python
def tokenize(text):
    # lowercase, keep words / @handles / #hashtags as single tokens
    return re.findall(r"[a-z0-9@#']+", text.lower())

PAD, UNK = "<pad>", "<unk>"
counter = Counter(tok for t in train_texts for tok in tokenize(t))
vocab = [PAD, UNK] + [w for w, _ in counter.most_common()]
stoi = {w: i for i, w in enumerate(vocab)}
itos = {i: w for w, i in stoi.items()}
VOCAB_SIZE = len(vocab)
print(f"vocab size = {VOCAB_SIZE}")
print("first 12 tokens:", vocab[:12])
```

- [ ] **Step 3: Add a code cell** encoding text → fixed-length id tensor:

```python
def encode(text):
    ids = [stoi.get(tok, stoi[UNK]) for tok in tokenize(text)][:BLOCK_SIZE]
    ids = ids + [stoi[PAD]] * (BLOCK_SIZE - len(ids))
    return ids

def make_tensors(texts, labels):
    X = torch.tensor([encode(t) for t in texts], dtype=torch.long)
    y = torch.tensor(labels, dtype=torch.long)
    return X, y

Xtr, ytr = make_tensors(train_texts, train_labels)
Xval, yval = make_tensors(val_texts, val_labels)
Xte, yte = make_tensors(test_texts, test_labels)

example = train_texts[0]
print("text :", example[:70])
print("tokens:", tokenize(example)[:12])
print("ids   :", encode(example)[:12])
print("X shape:", Xtr.shape, "(rows = tweets, cols = token positions)")
assert Xtr.shape == (len(train_texts), BLOCK_SIZE)
```

- [ ] **Step 4: Verify execution** (same nbconvert command). Expected `exit=0`; shows vocab size, a worked text→tokens→ids example, and `X shape`.

- [ ] **Step 5: Commit**

```bash
git add llm-model-layers-tutorial/notebook.ipynb
git commit -m "Add word-level tokenizer and tensor encoding cells"
```

---

## Task 5: Embeddings ("vectoring") and positional encoding

**Files:**
- Modify: `llm-model-layers-tutorial/notebook.ipynb`

- [ ] **Step 1: Add a markdown cell** "Embeddings: turning integers into vectors (a.k.a. 'vectoring')." Explain an embedding table is just a lookup matrix of shape `(VOCAB_SIZE, D_MODEL)`, one learnable row per token, and these rows ARE parameters that training will shape so similar words land near each other.

- [ ] **Step 2: Add a code cell** demonstrating a real embedding lookup:

```python
tok_emb = nn.Embedding(VOCAB_SIZE, D_MODEL)
print("embedding table shape:", tok_emb.weight.shape, "<- (vocab, d_model)")
print("parameters in this one table:", tok_emb.weight.numel())

ids = Xtr[:1]                      # one tweet: (1, BLOCK_SIZE)
vecs = tok_emb(ids)               # (1, BLOCK_SIZE, D_MODEL)
print("one tweet, embedded:", vecs.shape, "<- each token is now a 64-d vector")
assert vecs.shape == (1, BLOCK_SIZE, D_MODEL)
```

- [ ] **Step 3: Add a markdown cell** "Position: attention is order-blind." Explain that self-attention treats the input as a *set*, so we add a second learned table indexed by position (0..BLOCK_SIZE-1) to inject word order.

- [ ] **Step 4: Add a code cell**:

```python
pos_emb = nn.Embedding(BLOCK_SIZE, D_MODEL)
positions = torch.arange(BLOCK_SIZE)         # 0,1,2,...
x = tok_emb(ids) + pos_emb(positions)        # broadcast-add position to each token
print("token + position:", x.shape)
assert x.shape == (1, BLOCK_SIZE, D_MODEL)
```

- [ ] **Step 5: Verify execution** (nbconvert). Expected `exit=0`; shows shapes and the embedding-table parameter count.

- [ ] **Step 6: Commit**

```bash
git add llm-model-layers-tutorial/notebook.ipynb
git commit -m "Add embedding and positional encoding teaching cells"
```

---

## Task 6: Self-attention from scratch (light math, real shapes)

**Files:**
- Modify: `llm-model-layers-tutorial/notebook.ipynb`

- [ ] **Step 1: Add a markdown cell** walking the intuition: each token emits a **query** ("what am I looking for?"), a **key** ("what do I offer?"), and a **value** ("what I'll pass on"). Attention weight between two tokens = softmax over (query · key) similarity, scaled by `1/sqrt(d_head)`; the new representation of a token is the weighted sum of all values. Show the single formula `softmax(QKᵀ/√d) V` and name each tensor.

- [ ] **Step 2: Add a code cell** implementing ONE attention head by hand and printing shapes at each step:

```python
d_head = D_MODEL  # single head for this illustration
Wq, Wk, Wv = (nn.Linear(D_MODEL, d_head, bias=False) for _ in range(3))

q, k, v = Wq(x), Wk(x), Wv(x)                 # each (1, T, d_head)
scores = q @ k.transpose(-2, -1) / d_head**0.5  # (1, T, T): token-to-token similarity
weights = scores.softmax(dim=-1)              # rows sum to 1
out = weights @ v                             # (1, T, d_head): weighted blend of values

print("q,k,v:", q.shape)
print("scores (T x T):", scores.shape)
print("weights row sums (should be ~1):", weights[0].sum(-1)[:5].tolist())
print("attention output:", out.shape)
assert torch.allclose(weights.sum(-1), torch.ones_like(weights.sum(-1)), atol=1e-5)
assert out.shape == (1, BLOCK_SIZE, d_head)
```

- [ ] **Step 3: Add a short markdown cell** noting that real models run several such heads in parallel ("multi-head") so different heads can specialise, then concatenate — which the next cell packages into a reusable module.

- [ ] **Step 4: Verify execution** (nbconvert). Expected `exit=0`; weights rows sum to ~1; shapes printed.

- [ ] **Step 5: Commit**

```bash
git add llm-model-layers-tutorial/notebook.ipynb
git commit -m "Add from-scratch self-attention teaching cell"
```

---

## Task 7: The transformer block, full model, and parameter reconciliation

**Files:**
- Modify: `llm-model-layers-tutorial/notebook.ipynb`

- [ ] **Step 1: Add a markdown cell** "From one head to a layer." Explain a transformer **block** = multi-head attention + a position-wise MLP, each wrapped in a residual connection (`x = x + sublayer(x)`) and LayerNorm; a **layer** is one such block; the model stacks `N_LAYERS` of them. Then we mean-pool over real (non-pad) tokens and a linear **head** maps to 3 class scores (**logits**).

- [ ] **Step 2: Add a code cell** defining the modules:

```python
class MultiHeadAttention(nn.Module):
    def __init__(self, d_model, n_heads):
        super().__init__()
        assert d_model % n_heads == 0
        self.n_heads, self.d_head = n_heads, d_model // n_heads
        self.qkv = nn.Linear(d_model, 3 * d_model)
        self.proj = nn.Linear(d_model, d_model)

    def forward(self, x):
        B, T, C = x.shape
        q, k, v = self.qkv(x).split(C, dim=2)
        def heads(t): return t.view(B, T, self.n_heads, self.d_head).transpose(1, 2)
        q, k, v = heads(q), heads(k), heads(v)
        att = (q @ k.transpose(-2, -1) / self.d_head**0.5).softmax(-1)
        out = (att @ v).transpose(1, 2).contiguous().view(B, T, C)
        return self.proj(out)

class Block(nn.Module):
    def __init__(self, d_model, n_heads, d_ff, dropout):
        super().__init__()
        self.ln1, self.ln2 = nn.LayerNorm(d_model), nn.LayerNorm(d_model)
        self.attn = MultiHeadAttention(d_model, n_heads)
        self.mlp = nn.Sequential(nn.Linear(d_model, d_ff), nn.GELU(),
                                 nn.Linear(d_ff, d_model), nn.Dropout(dropout))
    def forward(self, x):
        x = x + self.attn(self.ln1(x))   # residual around attention
        x = x + self.mlp(self.ln2(x))    # residual around MLP
        return x

class TinyTransformerClassifier(nn.Module):
    def __init__(self):
        super().__init__()
        self.tok_emb = nn.Embedding(VOCAB_SIZE, D_MODEL, padding_idx=stoi[PAD])
        self.pos_emb = nn.Embedding(BLOCK_SIZE, D_MODEL)
        self.blocks = nn.ModuleList([Block(D_MODEL, N_HEADS, D_FF, DROPOUT)
                                     for _ in range(N_LAYERS)])
        self.ln_f = nn.LayerNorm(D_MODEL)
        self.head = nn.Linear(D_MODEL, N_CLASSES)

    def forward(self, idx):
        B, T = idx.shape
        x = self.tok_emb(idx) + self.pos_emb(torch.arange(T, device=idx.device))
        for blk in self.blocks:
            x = blk(x)
        x = self.ln_f(x)
        mask = (idx != stoi[PAD]).unsqueeze(-1).float()      # ignore pad in the average
        pooled = (x * mask).sum(1) / mask.sum(1).clamp(min=1)
        return self.head(pooled)                             # (B, N_CLASSES) logits

model = TinyTransformerClassifier().to(DEVICE)
logits = model(Xtr[:4].to(DEVICE))
print("logits shape:", logits.shape, "<- (batch, 3 class scores)")
assert logits.shape == (4, N_CLASSES)
```

- [ ] **Step 3: Add a code cell** that counts parameters AND reconciles a by-hand breakdown to the real total (this is the centrepiece teaching moment):

```python
def numel(m): return sum(p.numel() for p in m.parameters())

total = numel(model)
emb   = model.tok_emb.weight.numel() + model.pos_emb.weight.numel()
blocks = sum(numel(b) for b in model.blocks)
head  = numel(model.head) + numel(model.ln_f)

print(f"{'embeddings (token+pos)':28} {emb:>9,}")
print(f"{'transformer blocks':28} {blocks:>9,}")
print(f"{'final norm + head':28} {head:>9,}")
print(f"{'-'*38}")
print(f"{'hand-summed total':28} {emb+blocks+head:>9,}")
print(f"{'model.parameters() total':28} {total:>9,}")
assert emb + blocks + head == total, "hand breakdown must equal the real count!"
print(f"\nThe token embedding table alone is "
      f"{model.tok_emb.weight.numel()/total:.0%} of all parameters.")
```

- [ ] **Step 4: Verify execution** (nbconvert). Expected `exit=0`; the breakdown prints and the reconciliation assert passes.

**Offline fallback note:** if Tasks 3–4 could not run due to a blocked dataset download, temporarily set `VOCAB_SIZE=2000`, `Xtr=torch.randint(0,2000,(64,BLOCK_SIZE))`, `stoi={PAD:0,UNK:1}` style stubs in a scratch cell to confirm Tasks 5–7 model code executes and the param reconciliation holds. Remove the scratch cell before committing.

- [ ] **Step 5: Commit**

```bash
git add llm-model-layers-tutorial/notebook.ipynb
git commit -m "Add transformer block, full model, and parameter reconciliation"
```

---

## Task 8: Untrained baseline + training loop + loss curve

**Files:**
- Modify: `llm-model-layers-tutorial/notebook.ipynb`

- [ ] **Step 1: Add a markdown cell** "Before training: random numbers." Explain the parameters are currently random, so predictions should be near chance — we evaluate now to have a 'before' to compare against, and we'll watch a single weight change during training to make 'learning' concrete.

- [ ] **Step 2: Add a code cell** for the untrained check + grabbing one weight to watch:

```python
@torch.no_grad()
def accuracy(model, X, y):
    model.eval()
    preds = model(X.to(DEVICE)).argmax(1).cpu()
    return (preds == y).float().mean().item()

print(f"untrained val accuracy: {accuracy(model, Xval, yval):.3f} "
      f"(chance ≈ {1/N_CLASSES:.3f})")
watch_before = model.head.weight[0, 0].item()
print("a single head weight, before training:", watch_before)
```

- [ ] **Step 3: Add a markdown cell** "Training = nudging the numbers." Explain the loop: forward pass → cross-entropy loss → `.backward()` computes gradients → optimizer step nudges every parameter downhill. Define validation loss as the overfitting watchdog.

- [ ] **Step 4: Add a code cell** with the training loop and live train/val loss tracking:

```python
opt = torch.optim.AdamW(model.parameters(), lr=LR)

def iterate_batches(X, y, bs, shuffle=True):
    idx = torch.randperm(len(X)) if shuffle else torch.arange(len(X))
    for i in range(0, len(X), bs):
        j = idx[i:i+bs]
        yield X[j].to(DEVICE), y[j].to(DEVICE)

@torch.no_grad()
def eval_loss(model, X, y):
    model.eval()
    losses = [F.cross_entropy(model(xb), yb).item() for xb, yb in iterate_batches(X, y, BATCH_SIZE, shuffle=False)]
    return float(np.mean(losses))

train_hist, val_hist = [], []
for epoch in range(EPOCHS):
    model.train()
    ep_losses = []
    for xb, yb in iterate_batches(Xtr, ytr, BATCH_SIZE):
        loss = F.cross_entropy(model(xb), yb)
        opt.zero_grad(); loss.backward(); opt.step()
        ep_losses.append(loss.item())
    tr, vl = float(np.mean(ep_losses)), eval_loss(model, Xval, yval)
    train_hist.append(tr); val_hist.append(vl)
    print(f"epoch {epoch+1:2d}  train_loss={tr:.3f}  val_loss={vl:.3f}  "
          f"val_acc={accuracy(model, Xval, yval):.3f}")

assert val_hist[-1] < val_hist[0], "validation loss should drop — the model should learn"
```

- [ ] **Step 5: Add a code cell** showing the weight moved + the loss curve:

```python
watch_after = model.head.weight[0, 0].item()
print(f"same weight, after training: {watch_after:.5f} (was {watch_before:.5f}) "
      f"— a parameter literally changed")

plt.figure(figsize=(6, 4))
plt.plot(range(1, EPOCHS+1), train_hist, marker="o", label="train loss")
plt.plot(range(1, EPOCHS+1), val_hist, marker="o", label="validation loss")
plt.xlabel("epoch"); plt.ylabel("cross-entropy loss"); plt.legend(); plt.title("Learning curve")
plt.tight_layout(); plt.show()
```

- [ ] **Step 6: Verify execution** (nbconvert, allow up to 300s). Expected `exit=0`; per-epoch losses print, val loss drops (assert passes), the watched weight differs before/after.

- [ ] **Step 7: Commit**

```bash
git add llm-model-layers-tutorial/notebook.ipynb
git commit -m "Add untrained baseline, training loop, and loss curve"
```

---

## Task 9: Held-out test evaluation — metrics + confusion matrix

**Files:**
- Modify: `llm-model-layers-tutorial/notebook.ipynb`

- [ ] **Step 1: Add a markdown cell** "The honest score: the test set." Explain we only touch the test split now, once, and report accuracy vs the majority baseline plus macro precision/recall/F1 (macro = average across classes, so the big 'neutral' class can't hide poor minority-class performance).

- [ ] **Step 2: Add a code cell** computing predictions + metrics:

```python
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

@torch.no_grad()
def predict(model, X):
    model.eval()
    return model(X.to(DEVICE)).argmax(1).cpu().numpy()

test_pred = predict(model, Xte)
test_true = yte.numpy()

test_acc = accuracy_score(test_true, test_pred)
print(f"test accuracy: {test_acc:.3f}   (majority baseline: {baseline_acc:.3f})")
print()
print(classification_report(test_true, test_pred, target_names=LABELS, digits=3))
assert test_acc >= baseline_acc, "a trained model should beat always-guessing-majority"
```

- [ ] **Step 3: Add a code cell** for the confusion matrix plot:

```python
cm = confusion_matrix(test_true, test_pred)
fig, ax = plt.subplots(figsize=(4.5, 4))
im = ax.imshow(cm, cmap="Blues")
ax.set_xticks(range(N_CLASSES)); ax.set_xticklabels(LABELS, rotation=45, ha="right")
ax.set_yticks(range(N_CLASSES)); ax.set_yticklabels(LABELS)
ax.set_xlabel("predicted"); ax.set_ylabel("actual"); ax.set_title("Confusion matrix (test)")
for i in range(N_CLASSES):
    for j in range(N_CLASSES):
        ax.text(j, i, cm[i, j], ha="center",
                color="white" if cm[i, j] > cm.max()/2 else "black")
plt.tight_layout(); plt.show()
```

- [ ] **Step 4: Add a markdown aside** "Why these metrics — and why not for ChatGPT." Explain precision/recall/F1/accuracy need a discrete right answer per example, which classification provides. A *generative* LM predicts a probability distribution over the next token — there's no single right answer — so those models are judged by loss / **perplexity** instead. The user's original instinct (precision/recall) fits *this* task precisely because we made it classification.

- [ ] **Step 5: Verify execution** (nbconvert). Expected `exit=0`; test accuracy ≥ baseline (assert passes); classification report + confusion matrix render.

- [ ] **Step 6: Commit**

```bash
git add llm-model-layers-tutorial/notebook.ipynb
git commit -m "Add test-set evaluation, metrics, and confusion matrix"
```

---

## Task 10: Glossary recap, final full run, docs

**Files:**
- Modify: `llm-model-layers-tutorial/notebook.ipynb`
- Verify: `llm-model-layers-tutorial/README.md`

- [ ] **Step 1: Add a final markdown "Recap / glossary" cell** mapping each term to the concrete object the reader touched:
  - **Parameter** — a single number in a `weight`/embedding table; we counted them all and watched one change.
  - **Layer / block** — one `Block` (attention + MLP + residual + norm); the model stacked `N_LAYERS`.
  - **Embedding / "vectoring"** — the `tok_emb` lookup table turning token ids into vectors.
  - **Positional encoding** — the `pos_emb` table adding word order.
  - **Attention** — `softmax(QKᵀ/√d)V`; each token mixing in others by relevance.
  - **Logits** — the 3 raw class scores from `head`, before softmax.
  - **Training** — loss → `.backward()` → optimizer step.
  One closing line: real LLMs are this same machine, scaled up (bigger `D_MODEL`, more layers, subword tokens, billions of parameters) and trained to predict the next token instead of a class.

- [ ] **Step 2: Final clean full-notebook execution, in place** (so the committed notebook carries its outputs/plots, like the repo's other tutorial):

```bash
cd /home/user/samples/llm-model-layers-tutorial
time .venv-tutorial/bin/jupyter nbconvert --to notebook --execute notebook.ipynb \
  --output notebook.ipynb --ExecutePreprocessor.timeout=300
echo "exit=$?"
```

Expected: `exit=0`; wall time within ~1–2 min budget (if over, lower `TRAIN_SUBSET`/`EPOCHS` in the config cell and re-run). Confirm no `assert` failed.

- [ ] **Step 3: Sanity-check the executed notebook has outputs and no errors**

```bash
cd /home/user/samples
python3 -c "
import json
nb=json.load(open('llm-model-layers-tutorial/notebook.ipynb'))
errs=[o for c in nb['cells'] if c['cell_type']=='code' for o in c.get('outputs',[]) if o.get('output_type')=='error']
print('error outputs:', len(errs)); assert not errs
print('code cells:', sum(c['cell_type']=='code' for c in nb['cells']))
"
```

Expected: `error outputs: 0`.

- [ ] **Step 4: Add the README to the docs index** if applicable — check whether `docs/example_projects.md` lists project READMEs via snippets and add this project's README include following the existing pattern.

```bash
cd /home/user/samples && grep -n "imdb-genre-distilbert" docs/example_projects.md || echo "no index entry pattern — skip"
```

If a pattern exists, mirror it for `llm-model-layers-tutorial/README.md`.

- [ ] **Step 5: Commit and push**

```bash
git add llm-model-layers-tutorial/ docs/
git commit -m "Add glossary recap and finalize executed tutorial notebook"
git push -u origin claude/llm-model-layers-tutorial-4wcpmk
```

---

## Self-review (completed during planning)

- **Spec coverage:** intro/mental-model (T2) ✓; data + provided splits + baseline (T3) ✓; tokenization, vocab-on-train, pad/unk (T4) ✓; embeddings/"vectoring" (T5) ✓; positional encoding (T5) ✓; attention from scratch with light math (T6) ✓; multi-head + MLP + residual/norm (T7) ✓; stacked layers + pool + head (T7) ✓; parameter count reconciliation (T7) ✓; untrained baseline + mini-train + see-a-weight-change + loss curve (T8) ✓; test accuracy vs baseline + macro P/R/F1 + confusion matrix (T9) ✓; metric aside re generative vs classification (T9) ✓; glossary recap (T10) ✓; README convention + placement (T1/T10) ✓; runtime budget + verification (every task + T10) ✓.
- **Placeholder scan:** no TBD/TODO; every code step shows full code; verification commands are concrete with expected output.
- **Type/name consistency:** `stoi`/`itos`/`vocab`/`VOCAB_SIZE`, `encode`/`make_tensors`, `Xtr/ytr/Xval/yval/Xte/yte`, `TinyTransformerClassifier`/`Block`/`MultiHeadAttention`, `accuracy`/`predict`/`eval_loss`, `baseline_acc`, `train_hist`/`val_hist` are used consistently across tasks. Hyperparameter names match the config cell in T2.
