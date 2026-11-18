/// Clean Architecture AI & RAG Streaming Engine for Dart 3 and Flutter.
library resilient_ai_stream;

// Domain Contracts (Interfaces)
export 'src/domain/contracts/ai_stream_adapter.dart';
export 'src/domain/contracts/ai_stream_transport.dart';
export 'src/domain/contracts/json_healer.dart';
export 'src/domain/contracts/retry_policy.dart';

// Domain Entities (Immutable Value Objects)
export 'src/domain/entities/ai_stream_messages.dart';
export 'src/domain/entities/ai_stream_request.dart';
export 'src/domain/entities/stream_recovery.dart';
export 'src/domain/entities/ai_stream_snapshot.dart';
export 'src/domain/entities/ai_stream_usage.dart';
export 'src/domain/entities/knowledge_citation.dart';
export 'src/domain/entities/streaming_tool_block.dart';

// Domain Sealed Hierarchies (Dart 3 Exhaustive Pattern Matching)
export 'src/domain/events/ai_stream_event.dart';
export 'src/domain/failures/ai_stream_failure.dart';
export 'src/domain/states/ai_stream_state.dart';

// Core Components
export 'src/core/coalescer/frame_synced_coalescer.dart';
export 'src/core/coalescer/timer_coalescer.dart';
export 'src/core/healer/incremental_json_healer.dart';
export 'src/core/healer/partial_json_healer.dart';
export 'src/core/parser/raw_sse_event.dart';
export 'src/core/parser/utf8_sse_transformer.dart';
export 'src/core/resilience/exponential_backoff_policy.dart';

// Infrastructure Transports & Protocol Adapters
export 'src/infrastructure/adapters/claude_sse_adapter.dart';
export 'src/infrastructure/adapters/composite_ai_adapter.dart';
export 'src/infrastructure/adapters/custom_rag_adapter.dart';
export 'src/infrastructure/adapters/openai_responses_sse_adapter.dart';
export 'src/infrastructure/adapters/openai_sse_adapter.dart';
export 'src/infrastructure/transports/custom_callback_transport.dart';
export 'src/infrastructure/transports/default_http_transport.dart';

// Application Orchestrator
export 'src/application/resilient_ai_stream_client.dart';
