from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

from .tools import build_tool_specs, execute_tool, safe_parse_tool_arguments

DEFAULT_HISTORY_FOR_MODEL_LIMIT = 24
DEFAULT_HISTORY_MSG_MAX_CHARS = 1200
DEFAULT_MEMORY_SUMMARY_MAX_CHARS = 2200


def _trim_text(value: Any, max_chars: int) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    safe_max = max(int(max_chars), 1)
    return text[:safe_max]


def _history_for_model(
    history: list[dict[str, str]] | None,
    *,
    max_messages: int = DEFAULT_HISTORY_FOR_MODEL_LIMIT,
    max_chars_per_message: int = DEFAULT_HISTORY_MSG_MAX_CHARS,
) -> list[dict[str, str]]:
    if not isinstance(history, list):
        return []
    selected = history[-max(int(max_messages), 1) :]
    prepared: list[dict[str, str]] = []
    for row in selected:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", "")).strip().lower()
        content = _trim_text(row.get("content", ""), max_chars_per_message)
        if role not in {"user", "assistant"} or not content:
            continue
        prepared.append({"role": role, "content": content})
    return prepared


def _extract_assistant_content(message: dict[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    text_parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if str(item.get("type", "")).strip() not in {"text", "output_text"}:
            continue
        text_value = item.get("text")
        if isinstance(text_value, str):
            cleaned = text_value.strip()
            if cleaned:
                text_parts.append(cleaned)
            continue
        if isinstance(text_value, dict):
            nested = str(text_value.get("value", "")).strip()
            if nested:
                text_parts.append(nested)
    return "\n".join(text_parts).strip()


def _build_system_prompt(*, include_actions: bool) -> str:
    base = (
        "Eres COCO IA, analista senior de performance marketing multi-tenant. "
        "Responde en espanol natural, breve y clara. "
        "Mantienes continuidad de la conversacion con el historial entregado. "
        "Usa SOLO informacion del contexto y de las herramientas disponibles. "
        "No inventes metricas ni fechas. Si falta informacion, dilo explicitamente. "
        "Cuando la pregunta sea numerica o comparativa, consulta herramientas antes de responder. "
        "Si el usuario pide tabla, responde en Markdown con tabla."
    )
    if include_actions:
        return (
            base
            + " Incluye recomendaciones accionables solo cuando el usuario las pida o cuando aporten valor claro."
        )
    return base + " No incluyas recomendaciones ni proximos pasos."


def _openai_chat_completion(
    *,
    api_key: str,
    payload: dict[str, Any],
    timeout_seconds: int,
) -> tuple[dict[str, Any], str]:
    req = urllib.request.Request(
        url="https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    last_error = ""
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=max(int(timeout_seconds), 10)) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw) if raw else {}
            if isinstance(parsed, dict):
                return parsed, ""
            last_error = "OpenAI devolvio un payload no compatible."
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            last_error = f"OpenAI HTTP {exc.code}: {body[:320]}"
            if exc.code in {429, 500, 502, 503, 504} and attempt < 2:
                time.sleep(1.0 * (attempt + 1))
                continue
            return {}, last_error
        except Exception as exc:
            last_error = f"No se pudo consultar OpenAI: {exc}"
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
                continue
            return {}, last_error
    return {}, last_error or "OpenAI no devolvio respuesta valida."


def run_coco_agent_turn(
    *,
    api_key: str,
    model: str,
    question: str,
    context: dict[str, Any],
    conversation_history: list[dict[str, str]] | None = None,
    memory_summary: str = "",
    include_actions: bool = True,
    max_tool_rounds: int = 6,
    temperature: float = 0.2,
    timeout_seconds: int = 45,
) -> tuple[str, int, int, str]:
    if not str(api_key or "").strip():
        return "", 0, 0, "OPENAI_API_KEY no esta configurada."
    safe_model = str(model or "").strip()
    if not safe_model:
        return "", 0, 0, "Modelo OpenAI no configurado."

    system_prompt = _build_system_prompt(include_actions=include_actions)
    context_json = json.dumps(context if isinstance(context, dict) else {}, ensure_ascii=False)
    scope_mode = str(
        ((context if isinstance(context, dict) else {}).get("scope", {}) or {}).get("mode", "total")
    ).strip().lower() or "total"

    messages: list[dict[str, Any]] = [{"role": "system", "content": system_prompt}]
    memory_text = _trim_text(memory_summary, DEFAULT_MEMORY_SUMMARY_MAX_CHARS)
    if memory_text:
        messages.append(
            {
                "role": "system",
                "content": f"Resumen breve de conversacion previa:\n{memory_text}",
            }
        )
    history_payload = _history_for_model(conversation_history)
    if history_payload:
        messages.extend(history_payload)
    messages.append(
        {
            "role": "user",
            "content": (
                f"Alcance activo para esta respuesta: {scope_mode}.\n"
                "Contexto analitico (JSON):\n"
                f"{context_json}\n\n"
                f"Pregunta del usuario:\n{str(question or '').strip()}"
            ),
        }
    )

    tools = build_tool_specs()
    total_prompt_tokens = 0
    total_completion_tokens = 0
    rounds = max(int(max_tool_rounds), 1)
    for _ in range(rounds):
        payload = {
            "model": safe_model,
            "temperature": float(temperature),
            "messages": messages,
            "tools": tools,
            "tool_choice": "auto",
        }
        parsed, err = _openai_chat_completion(
            api_key=api_key,
            payload=payload,
            timeout_seconds=timeout_seconds,
        )
        if err:
            return "", total_prompt_tokens, total_completion_tokens, err
        usage = parsed.get("usage", {}) if isinstance(parsed, dict) else {}
        try:
            total_prompt_tokens += int(usage.get("prompt_tokens") or 0)
        except Exception:
            pass
        try:
            total_completion_tokens += int(usage.get("completion_tokens") or 0)
        except Exception:
            pass

        choices = parsed.get("choices", []) if isinstance(parsed, dict) else []
        if not isinstance(choices, list) or not choices:
            return (
                "",
                total_prompt_tokens,
                total_completion_tokens,
                "OpenAI no devolvio choices en la respuesta.",
            )
        first_choice = choices[0] if isinstance(choices[0], dict) else {}
        message = first_choice.get("message", {}) if isinstance(first_choice, dict) else {}
        if not isinstance(message, dict):
            return (
                "",
                total_prompt_tokens,
                total_completion_tokens,
                "OpenAI devolvio un mensaje invalido.",
            )

        tool_calls = message.get("tool_calls", [])
        assistant_text = _extract_assistant_content(message)
        if not isinstance(tool_calls, list):
            tool_calls = []

        if tool_calls:
            messages.append(
                {
                    "role": "assistant",
                    "content": message.get("content"),
                    "tool_calls": tool_calls,
                }
            )
            for tool_call in tool_calls:
                if not isinstance(tool_call, dict):
                    continue
                tool_call_id = str(tool_call.get("id", "")).strip()
                function_block = tool_call.get("function", {})
                if not isinstance(function_block, dict):
                    function_block = {}
                tool_name = str(function_block.get("name", "")).strip()
                raw_arguments = function_block.get("arguments", "{}")
                arguments = safe_parse_tool_arguments(raw_arguments)
                tool_result = execute_tool(
                    tool_name=tool_name,
                    arguments=arguments,
                    context=context if isinstance(context, dict) else {},
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call_id,
                        "content": json.dumps(tool_result, ensure_ascii=False),
                    }
                )
            continue

        if assistant_text:
            return assistant_text, total_prompt_tokens, total_completion_tokens, ""

        return (
            "",
            total_prompt_tokens,
            total_completion_tokens,
            "OpenAI no devolvio contenido en la respuesta.",
        )

    return (
        "",
        total_prompt_tokens,
        total_completion_tokens,
        "Se alcanzo el maximo de rondas de herramientas sin respuesta final.",
    )
