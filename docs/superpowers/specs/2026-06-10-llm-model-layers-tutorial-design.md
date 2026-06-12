# LLM Model Layers Tutorial — Design

**Date:** 2026-06-10
**Status:** Approved (pending spec review)
**Branch:** `claude/llm-model-layers-tutorial-4wcpmk`

## Goal

A self-contained, Colab-runnable Jupyter notebook that teaches a non-expert
what an LLM-style transformer actually *is* by building a tiny one from
scratch in PyTorch and training it on a real dataset. The reader should come
away able to point at every piece of jargon — **parameters, layers,
embeddings/"vectoring", positional encoding, attention, logits** — and say
what it is and where it lives in the code.

Pedagogical stance: **intuition + light math**. Real equations where they
clarify (dot product, softmax), but always paired with the actual tensor and
its printed shape. No hand-waving, no unexplained magic.

## Audience & success criteria

- Audience: someone comfortable reading Python who has heard the terms but
  never seen the internals.
- Success: the notebook runs top-to-bottom on a free Colab CPU (or locally)
  in ~1–2 minutes, the model visibly learns (validation loss drops, test
  accuracy beats the majority-class baseline), and every section ties a
  concept to a concrete object the reader just inspected.

## The running example

A **tiny transformer text classifier built from scratch**, trained on a real,
open-source **3-class tweet sentiment** task.

Why classification (not generation): it makes precision/recall/F1 and a
confusion matrix genuinely meaningful, and pairs naturally with a
train/validation/test split — both of which the user explicitly wants. The
transformer internals being taught are identical to a generative model; only
the head differs (pool the sequence + linear classifier instead of next-token
prediction).

### Data

- **`tweet_eval`, `sentiment` subset** (SemEval-2017 Task 4), loaded via
  `datasets.load_dataset("tweet_eval", "sentiment")` — the same HF `datasets`
  mechanism the repo's existing `imdb-genre-distilbert` notebook uses.
- Real, open-source tweets. **3 classes**: `0=negative`, `1=neutral`,
  `2=positive`.
- Ships **pre-labeled train / validation / test** splits
  (~45,615 / 2,000 / 12,284). We use the authors' splits directly and explain
  each split's job (train = learn, validation = watch for overfitting / pick
  when to stop, test = one final honest score). Teaching point: *use the
  splits the dataset authors gave you.*
- For the CPU/time budget we **subsample** the train split (e.g. a few
  thousand seeded examples) and cap validation/test similarly; the full splits
  are available but unnecessary for a teaching run. Subsample sizes live in the
  config cell.
- **Word-level tokenization** (one token = one word), intuitive for short text
  and a good teaching contrast to subword tokenizers. Light normalization
  (lowercase, simple regex split; tweets keep `@user`/hashtag tokens as-is).
  Vocab built from the (subsampled) train split only — another teaching point:
  *the vocabulary is fit on train, not on val/test.* An `<unk>` token handles
  unseen words; phrases padded/truncated to a max length with a `<pad>` token.
- Note class balance up front: tweet sentiment skews toward neutral, so we
  report a **majority-class baseline** and prefer **macro-averaged** P/R/F1 so
  the metric isn't dominated by the largest class.

### Architecture (deliberately tiny, so parameter counts reconcile by hand)

- Token embedding table + learned positional embedding table.
- N transformer blocks (default ~2), each: multi-head self-attention
  (default ~2 heads) + position-wise feed-forward MLP, with residual
  connections and LayerNorm.
- Mean-pool over the (non-padding) token positions → a single sequence vector.
- Linear classification head → **3 logits** (negative / neutral / positive).
- Small dims (e.g. `d_model=64`) so the total parameter count is small and
  every component's contribution can be added up by hand and reconciled
  against `sum(p.numel() ...)`. (Note: with a vocab of a few thousand words,
  the token-embedding table dominates the parameter count — itself a useful
  teaching observation.)

### Metrics

- Train + validation **loss curves** (matplotlib) during training.
- Final held-out **test set**: accuracy vs. majority-class baseline,
  **macro precision / recall / F1**, and a **3×3 confusion matrix**.
- A short markdown aside on *why* these classification metrics apply here but
  would not apply to a generative language model (which uses
  loss/perplexity) — turning the user's original question into a teaching
  point.

## Notebook structure (cells)

1. **Title + intro** — "A model is a big pile of numbers (parameters)
   arranged into functions (layers)." Frames the whole notebook.
2. **Install + imports + config** — `torch`, `datasets`, `matplotlib`,
   `scikit-learn` (metrics/confusion matrix); seed; all hyperparameters and
   subsample sizes in one place.
3. **The task & the data** — load `tweet_eval/sentiment`; show example tweets
   per class; note the provided train/val/test splits and each one's role;
   note class imbalance + majority baseline.
4. **Text → tokens** — word-level tokenization, vocab fit on train, `stoi`/
   `itos`, `<unk>`/`<pad>`, padding/truncation. "Symbols become integers."
5. **Embeddings = "vectoring"** — integer → vector via a lookup table; print
   the table and its shape; show it *is* learnable parameters.
6. **Positional encoding** — why self-attention is order-blind; add a learned
   position vector.
7. **Self-attention from scratch** — Q/K/V, dot-product similarity, softmax
   weights, weighted sum. Light math + printed shapes at each step.
8. **Multi-head + feed-forward MLP** — the rest of a transformer block
   (residual + LayerNorm).
9. **Stacking → layers + classification head** — a "layer" is one block; the
   model is N blocks stacked, then mean-pool + linear head → 3 logits.
10. **Assemble the full model + count parameters** — break the total down by
    component (embeddings vs. attention vs. MLP vs. head) and reconcile to the
    printed `numel()` total. Demystifies "X-million parameters" and shows the
    embedding table's dominance.
11. **Untrained baseline** — predict on validation; show it's ~majority-class /
    random.
12. **Mini-train loop** — loss, `.backward()`, optimizer step; plot train +
    val loss; print one specific weight before vs. after to *see* a parameter
    update.
13. **Evaluate on the held-out test set** — accuracy vs. baseline, macro
    precision/recall/F1, confusion matrix; the metric aside described above.
14. **Glossary recap** — map each jargon term back to the concrete object the
    reader touched.

## Files & placement

Follows the repo's existing notebook-project convention
(`imdb-genre-distilbert/`: `README.md` + `notebook.ipynb`, no nested
`@job/@task` package).

```
llm-model-layers-tutorial/
├── notebook.ipynb   # the tutorial
└── README.md        # title (setext), one-paragraph description, run command
```

`README.md` follows the project README convention: setext title, one concise
paragraph, a bash code block with the Colab link + local `jupyter lab` command.

## Dependencies

`torch`, `datasets`, `matplotlib`, `scikit-learn`. All available on Colab by
default; the install cell is a `pip install -q` guard for local runs.

## Testing / verification

- Execute the notebook end-to-end (e.g. `jupyter nbconvert --to notebook
  --execute`) and confirm: no errors, validation loss decreases, test accuracy
  beats the majority-class baseline, and the printed parameter breakdown sums
  to the reported total.
- Confirm runtime is within the ~1–2 min CPU budget (tune subsample sizes /
  epochs if needed).

## Non-goals (YAGNI)

- No attention heatmaps or 2D embedding projections (user chose
  inspect + mini-train, not the heavy-viz option).
- No pretrained weights, no pretrained model downloads, no GPU requirement.
  (The only runtime download is the small `tweet_eval` dataset.)
- No generative/next-token head.
- No subword/BPE tokenizer — word-level only, for clarity.
