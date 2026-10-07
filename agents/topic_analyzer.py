"""
HN Topic Analyzer Agent

Deployed to: StreamNative Agent Engine
Agent ID: agt_EF5DQG4F7H4YXS9HFWCF
Model: Claude 3.5 Sonnet

Purpose:
--------
Analyzes Hacker News comment discussions to discover emerging topics not
obvious from the story title. Runs as a session in the StreamNative Agent Engine.

Inputs:
-------
HN comment events from Kafka topic 'hn-events' with structure:
  {
    "kind": "UPDATE",
    "ts": timestamp,
    "item": {
      "id": <int>,
      "parent": <parent-story-id>,
      "text": "<comment-text>",
      "by": "<username>",
      "score": <int>
    }
  }

Output:
-------
JSON to Kafka topic (via Agent Engine) with structure:
  {
    "story_id": <int>,
    "title": "<title>",
    "emerging_topics": ["model compression", "quantization", ...],
    "ts": <timestamp>
  }

Emerging Topics Examples:
------------------------
- Model compression, quantization, parameter efficiency
- Edge deployment, inference optimization
- Specific tools and frameworks (e.g., "vLLM", "ollama", "TensorRT")
- Methodologies and techniques
- Problem domains (e.g., "context window reduction", "token prediction")

Agent System Prompt:
-------------------
"Analyze HN comment discussions for emerging topics. Extract technical concepts
NOT obvious from title: 'model compression', 'quantization', specific tools,
methodologies. Respond with JSON: {\"story_id\": <int>, \"title\": \"<title>\",
\"emerging_topics\": [<strings>], \"ts\": <timestamp>}"

Deployment:
-----------
Created via ork CLI on 2026-10-07:
  ork agent create \\
    --name hn-topic-analyzer \\
    --model claude-3-5-sonnet-20241022 \\
    --registry-url https://fw-9604dc75d2a7.gcp-shared-usce1.g.snio.cloud \\
    --access-token <JWT_TOKEN> \\
    --system "<prompt above>"

Session ID: ses_3FXJW4X2D56SWN97FVD7
Environment ID: env_MM8SNVRL2ZZYRLJE0E5W
Status: Running on Team 3 cluster (o-1a54s)
"""

import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def extract_emerging_topics(text: str, title: str = "") -> list[str]:
    """
    Extract emerging topics from HN comment text.
    Looks for technical concepts, tools, and methodologies.
    This is the logic Claude 3.5 Sonnet agent implements.
    """
    topics = []
    text_lower = text.lower()
    title_lower = title.lower()

    # Emerging topic keywords and phrases
    emerging_topics_map = {
        "model compression": [
            "model compression",
            "model pruning",
            "knowledge distillation",
        ],
        "quantization": ["quantization", "int8", "fp8", "bfloat16"],
        "parameter efficiency": [
            "parameter efficient",
            "lora",
            "adapter",
            "prefix tuning",
        ],
        "edge deployment": [
            "edge deployment",
            "on-device",
            "mobile inference",
            "edge ai",
        ],
        "inference optimization": [
            "inference optimization",
            "latency reduction",
            "throughput",
            "batching",
        ],
        "context window": [
            "context window",
            "long context",
            "sliding window",
            "token limit",
        ],
        "token prediction": [
            "token prediction",
            "speculative decoding",
            "draft model",
        ],
        "vLLM": ["vlm", "vllm"],
        "ollama": ["ollama"],
        "TensorRT": ["tensorrt"],
        "ONNX": ["onnx"],
        "huggingface": ["huggingface", "transformers library"],
        "prompt engineering": ["prompt engineering", "prompt injection"],
        "RAG": ["retrieval augmented", "rag"],
        "fine-tuning": ["fine-tuning", "finetuning", "sft"],
    }

    for topic, keywords in emerging_topics_map.items():
        # Don't include topics already obvious from the title
        if any(keyword in title_lower for keyword in keywords):
            continue

        # Check if topic is mentioned in comments
        if any(keyword in text_lower for keyword in keywords):
            topics.append(topic)

    return topics


def process_comment_event(event: dict, story_title: str = "") -> Optional[dict]:
    """
    Process an HN comment event and extract emerging topics.
    Agent session will call this for each comment.
    """
    if event.get("kind") != "UPDATE":
        return None

    item = event.get("item", {})
    text = item.get("text", "")
    story_id = item.get("parent")

    if not text or not story_id:
        return None

    # Skip very short comments
    if len(text) < 20:
        return None

    emerging_topics = extract_emerging_topics(text, story_title)

    if not emerging_topics:
        return None

    return {
        "story_id": story_id,
        "title": story_title,
        "emerging_topics": emerging_topics,
        "ts": event.get("ts"),
    }


def aggregate_topics(topic_events: list[dict]) -> dict:
    """
    Aggregate emerging topics across multiple comments on the same story.
    Returns topic frequency and top emerging topics.
    """
    topic_counts = {}

    for event in topic_events:
        for topic in event.get("emerging_topics", []):
            topic_counts[topic] = topic_counts.get(topic, 0) + 1

    return {
        "total_mentions": sum(topic_counts.values()),
        "unique_topics": len(topic_counts),
        "topics_by_frequency": sorted(
            topic_counts.items(), key=lambda x: x[1], reverse=True
        ),
    }
