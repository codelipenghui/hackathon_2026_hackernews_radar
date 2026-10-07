# HN Radar Agents

Two Claude 3.5 Sonnet agents running on StreamNative Agent Engine process HN events in real-time.

## Agents

### 1. Subject Extractor
- **Agent ID**: agt_5017ZCP32NVMF970W3H3
- **Session ID**: ses_3NXDES7WP15KH0CPP33C
- **File**: `subject_extractor.py`
- **Purpose**: Extract technical subjects/domains from HN story titles

**Input**: HN story events (NEW, UPDATE)
```json
{
  "kind": "NEW",
  "item": {
    "id": 42187502,
    "title": "Rust's Memory Safety and LLM Model Inference",
    "url": "...",
    "score": 150
  }
}
```

**Output**: Extracted subjects
```json
{
  "story_id": 42187502,
  "title": "Rust's Memory Safety and LLM Model Inference",
  "subjects": ["Rust", "LLM", "AI"]
}
```

**Subjects Recognized**:
- AI / LLM
- Rust / Python / Go / TypeScript / React
- DevOps / Cloud / Database
- Security / Blockchain / Quantum
- WebAssembly / GraphQL

---

### 2. Topic Analyzer
- **Agent ID**: agt_EF5DQG4F7H4YXS9HFWCF
- **Session ID**: ses_3FXJW4X2D56SWN97FVD7
- **File**: `topic_analyzer.py`
- **Purpose**: Discover emerging topics from HN comment discussions

**Input**: HN comment events
```json
{
  "kind": "UPDATE",
  "item": {
    "id": 42187603,
    "parent": 42187502,
    "text": "The vLLM project uses quantization and LoRA adapters for edge deployment...",
    "by": "user123",
    "score": 25
  }
}
```

**Output**: Emerging topics
```json
{
  "story_id": 42187502,
  "title": "Rust's Memory Safety and LLM Model Inference",
  "emerging_topics": ["quantization", "LoRA", "edge deployment", "vLLM"],
  "ts": 1728338413000
}
```

**Emerging Topics Detected**:
- Model compression, quantization, parameter efficiency
- Edge deployment, inference optimization
- Context window, token prediction
- Tools: vLLM, ollama, TensorRT, ONNX, Hugging Face
- Fine-tuning, prompt engineering, RAG

---

## Deployment

Both agents run as sessions in StreamNative Agent Engine on Team 3 cluster (o-1a54s).

### Environment
- **Name**: hn-radar-env
- **ID**: env_MM8SNVRL2ZZYRLJE0E5W
- **Endpoint**: fw-9604dc75d2a7.gcp-shared-usce1.g.snio.cloud

### Setup Steps

See main README.md for complete setup. Quick reference:

```bash
# 1. Build ork CLI (Go 1.25+)
git clone https://github.com/orca-ae/orca-cli
cd orca-cli
go install ./cmd/ork

# 2. Create environment
export ORCA_BASE_URL=https://fw-9604dc75d2a7.gcp-shared-usce1.g.snio.cloud
export ORCA_ACCESS_TOKEN=<your-api-key>

ork agent environments create \
  --name hn-radar-env \
  --registry-url $ORCA_BASE_URL \
  --access-token "$ORCA_ACCESS_TOKEN"

# 3. Create agents (IDs already deployed above)
# See subject_extractor.py and topic_analyzer.py for full agent creation commands
```

---

## How Agents Work

1. **Subject Extractor**
   - Listens to `hn-events` Kafka topic
   - Receives story title in each event
   - Uses Claude 3.5 Sonnet to extract matching technical subjects
   - Outputs to Kafka topic (configurable via Agent Engine)

2. **Topic Analyzer**
   - Listens to `hn-events` Kafka topic for comments
   - Analyzes comment text for emerging technical concepts
   - Ignores topics already obvious from story title
   - Groups topics from related comments
   - Outputs to Kafka topic (configurable via Agent Engine)

---

## Integration with Dashboard

The dashboard at localhost:8080 can query RisingWave materialized views built from agent outputs:

```sql
-- Subject trends (from subject extractor output)
CREATE MATERIALIZED VIEW hn_subjects_mv AS
SELECT * FROM "hn-subjects" WHERE extracted_at > now() - interval '1 hour';

-- Emerging topics (from topic analyzer output)
CREATE MATERIALIZED VIEW hn_topics_mv AS
SELECT * FROM "hn-topics" WHERE ts > now() - interval '1 hour';

-- Top subjects
SELECT subject, COUNT(*) as mentions
FROM hn_subjects_mv
GROUP BY subject
ORDER BY mentions DESC;

-- Top emerging topics
SELECT topic, COUNT(*) as mentions
FROM hn_topics_mv
GROUP BY topic
ORDER BY mentions DESC;
```

---

## Testing Agents Locally

For local testing without deploying to StreamNative:

```python
from agents.subject_extractor import extract_subjects, process_hn_event
from agents.topic_analyzer import extract_emerging_topics, aggregate_topics

# Test subject extractor
event = {
    "kind": "NEW",
    "item": {
        "id": 1,
        "title": "Building LLM Applications with Rust",
        "score": 100
    }
}
result = process_hn_event(event)
print(result)  # {"story_id": 1, "subjects": ["LLM", "Rust"], ...}

# Test topic analyzer
topics = extract_emerging_topics(
    "vLLM with quantization for edge deployment on mobile",
    "Building LLM Applications"
)
print(topics)  # ["vLLM", "quantization", "edge deployment"]
```

---

## Agent Versions

- Subject Extractor: v1 (Claude 3.5 Sonnet)
- Topic Analyzer: v1 (Claude 3.5 Sonnet)

Updates: Create new versions via `ork agent create` with updated system prompt.

---

## Monitoring

Check agent sessions:

```bash
ork agent sessions list \
  --registry-url $ORCA_BASE_URL \
  --access-token "$ORCA_ACCESS_TOKEN" \
  -o json | jq '.data[] | {id, title, agent}'
```

View session events:

```bash
ork agent sessions events stream \
  --session ses_3NXDES7WP15KH0CPP33C \
  --registry-url $ORCA_BASE_URL \
  --access-token "$ORCA_ACCESS_TOKEN"
```

---

## Files Reference

- `subject_extractor.py` - Subject extraction agent implementation
- `topic_analyzer.py` - Topic analysis agent implementation
- `AGENTS.md` - This file
- See main `README.md` for dashboard and producer setup
