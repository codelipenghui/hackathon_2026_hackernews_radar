"""
HN Subject Extractor Agent

Deployed to: StreamNative Agent Engine
Agent ID: agt_5017ZCP32NVMF970W3H3
Model: Claude 3.5 Sonnet

Purpose:
--------
Analyzes Hacker News story titles to extract technical subjects/domains.
Runs as a session in the StreamNative Agent Engine.

Inputs:
-------
HN story events from Kafka topic 'hn-events' with structure:
  {
    "kind": "NEW" | "UPDATE",
    "ts": timestamp,
    "item": {
      "id": <int>,
      "title": "<story-title>",
      "url": "<url>",
      "score": <int>,
      "by": "<username>"
    }
  }

Output:
-------
JSON to Kafka topic (via Agent Engine) with structure:
  {
    "story_id": <int>,
    "title": "<title>",
    "subjects": ["AI", "LLM", "Rust", ...]
  }

Subjects Recognized:
--------------------
- AI / LLM
- Rust / Python / Go / TypeScript / React
- DevOps / Cloud / Database
- Security / Blockchain / Quantum
- WebAssembly / GraphQL

Agent System Prompt:
-------------------
"You are analyzing Hacker News stories to extract technical subjects. Subjects:
AI, LLM, Rust, Python, DevOps, React, TypeScript, Go, Database, Security,
Blockchain, Quantum, WebAssembly, GraphQL, Cloud. For each story, identify
matching subjects and respond with JSON: {\"story_id\": <int>, \"title\":
\"<title>\", \"subjects\": [<strings>]}"

Deployment:
-----------
Created via ork CLI on 2026-10-07:
  ork agent create \\
    --name hn-subject-extractor \\
    --model claude-3-5-sonnet-20241022 \\
    --registry-url https://fw-9604dc75d2a7.gcp-shared-usce1.g.snio.cloud \\
    --access-token <JWT_TOKEN> \\
    --system "<prompt above>"

Session ID: ses_3NXDES7WP15KH0CPP33C
Environment ID: env_MM8SNVRL2ZZYRLJE0E5W
Status: Running on Team 3 cluster (o-1a54s)
"""

import json
import logging

logger = logging.getLogger(__name__)


def extract_subjects(title: str) -> list[str]:
    """
    Extract subjects from an HN story title.
    This is the logic Claude 3.5 Sonnet agent implements.
    """
    subjects = []
    title_lower = title.lower()

    # Subject keywords
    subjects_map = {
        "AI": ["ai", "artificial intelligence", "machine learning", "ml"],
        "LLM": ["llm", "language model", "gpt", "claude", "transformer"],
        "Rust": ["rust"],
        "Python": ["python"],
        "Go": ["golang", "go"],
        "TypeScript": ["typescript", "ts"],
        "React": ["react", "reactjs"],
        "DevOps": ["devops", "kubernetes", "docker", "ci/cd", "deployment"],
        "Cloud": ["aws", "gcp", "azure", "cloud", "serverless"],
        "Database": ["database", "postgres", "mysql", "mongodb", "sql"],
        "Security": ["security", "encryption", "ssl", "tls", "auth"],
        "Blockchain": ["blockchain", "crypto", "ethereum", "bitcoin", "web3"],
        "Quantum": ["quantum", "quantum computing"],
        "WebAssembly": ["wasm", "webassembly"],
        "GraphQL": ["graphql"],
    }

    for subject, keywords in subjects_map.items():
        if any(keyword in title_lower for keyword in keywords):
            subjects.append(subject)

    return subjects


def process_hn_event(event: dict) -> dict:
    """
    Process an HN event and extract subjects.
    Agent session will call this for each story.
    """
    if event.get("kind") not in ("NEW", "UPDATE"):
        return None

    item = event.get("item", {})
    title = item.get("title", "")
    story_id = item.get("id")

    if not title or not story_id:
        return None

    subjects = extract_subjects(title)

    return {
        "story_id": story_id,
        "title": title,
        "subjects": subjects,
        "extracted_at": event.get("ts"),
    }
