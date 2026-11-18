import 'dart:convert';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';
import 'package:test/test.dart';

Stream<List<int>> sse(List<String> frames) =>
    Stream.value(utf8.encode(frames.join()));

String data(Object o, {String? event}) =>
    '${event == null ? '' : 'event: $event\n'}data: ${jsonEncode(o)}\n\n';

ResilientAiStreamClient client(
  AiStreamAdapter adapter,
  List<String> frames, {
  AiStreamMessages messages = AiStreamMessages.english,
}) =>
    ResilientAiStreamClient(
      transport:
          CustomCallbackTransport(onOpenStream: (_) async => sse(frames)),
      adapter: adapter,
      uiCoalesceInterval: Duration.zero,
      messages: messages,
    );

final request = AiStreamRequest(url: Uri.parse('https://example.com/s'));

void main() {
  group('OpenAiSseAdapter (Chat Completions)', () {
    test('assembles streamed tool calls, usage and finish reason', () async {
      final c = client(const OpenAiSseAdapter(), [
        data({
          'id': 'chatcmpl-1',
          'choices': [
            {
              'index': 0,
              'delta': {
                'role': 'assistant',
                'tool_calls': [
                  {
                    'index': 0,
                    'id': 'call_a',
                    'function': {'name': 'search', 'arguments': ''}
                  }
                ]
              }
            }
          ]
        }),
        data({
          'choices': [
            {
              'index': 0,
              'delta': {
                'tool_calls': [
                  {
                    'index': 0,
                    'function': {'arguments': '{"q":"leave pol'}
                  }
                ]
              }
            }
          ]
        }),
        data({
          'choices': [
            {
              'index': 0,
              'delta': {
                'tool_calls': [
                  {
                    'index': 0,
                    'function': {'arguments': 'icy","k":3}'}
                  }
                ]
              },
              'finish_reason': 'tool_calls'
            }
          ]
        }),
        data({
          'choices': <Object>[],
          'usage': {
            'prompt_tokens': 10,
            'completion_tokens': 5,
            'total_tokens': 15,
            'prompt_tokens_details': {'cached_tokens': 4}
          }
        }),
        'data: [DONE]\n\n',
      ]);
      final states = await c.stream(request).toList();
      final snap = states.last.snapshot;
      expect(states.last, isA<AiStreamCompleted>());
      expect(snap.messageId, 'chatcmpl-1');
      final tool = snap.toolBlocks[0]!;
      expect(tool.id, 'call_a');
      expect(tool.name, 'search');
      expect(tool.isComplete, isTrue);
      expect(jsonDecode(tool.rawPartialJson), {'q': 'leave policy', 'k': 3});
      expect(tool.healedArguments, {'q': 'leave policy', 'k': 3});
      expect(snap.finishReason, 'tool_calls');
      expect(snap.usage!.inputTokens, 10);
      expect(snap.usage!.totalTokens, 15);
      expect(snap.usage!.cachedInputTokens, 4);
      await c.dispose();
    });

    test('parallel tool calls keep separate blocks', () async {
      final c = client(const OpenAiSseAdapter(), [
        data({
          'choices': [
            {
              'index': 0,
              'delta': {
                'tool_calls': [
                  {
                    'index': 0,
                    'id': 'a',
                    'function': {'name': 'one', 'arguments': '{"x":1}'}
                  },
                  {
                    'index': 1,
                    'id': 'b',
                    'function': {'name': 'two', 'arguments': '{"y":2}'}
                  },
                ]
              },
              'finish_reason': 'tool_calls'
            }
          ]
        }),
        'data: [DONE]\n\n',
      ]);
      final snap = (await c.stream(request).toList()).last.snapshot;
      expect(snap.toolBlocks.keys, [0, 1]);
      expect(snap.toolBlocks[1]!.name, 'two');
      expect(snap.toolBlocks[1]!.healedArguments, {'y': 2});
      await c.dispose();
    });

    test('only the selected choice index is surfaced', () async {
      final c = client(const OpenAiSseAdapter(choiceIndex: 1), [
        data({
          'choices': [
            {
              'index': 0,
              'delta': {'content': 'zero'}
            },
            {
              'index': 1,
              'delta': {'content': 'one'}
            },
          ]
        }),
        'data: [DONE]\n\n',
      ]);
      expect((await c.stream(request).toList()).last.snapshot.text, 'one');
      await c.dispose();
    });

    test('reasoning deltas and in-band errors', () async {
      final ok = client(const OpenAiSseAdapter(), [
        data({
          'choices': [
            {
              'index': 0,
              'delta': {'reasoning_content': 'hmm', 'content': 'hi'}
            }
          ]
        }),
        'data: [DONE]\n\n',
      ]);
      final snap = (await ok.stream(request).toList()).last.snapshot;
      expect(snap.thinkingText, 'hmm');
      expect(snap.text, 'hi');
      await ok.dispose();

      final bad = client(const OpenAiSseAdapter(), [
        data({
          'error': {'type': 'rate_limit', 'message': 'slow down'}
        }),
      ]);
      final last = (await bad.stream(request).toList()).last as AiStreamFailed;
      expect(last.failure, isA<ProtocolParseStreamFailure>());
      expect(last.failure.message, contains('slow down'));
      await bad.dispose();
    });
  });

  group('OpenAiResponsesSseAdapter', () {
    test('text, reasoning, function call, citation, usage, completion',
        () async {
      final c = client(const OpenAiResponsesSseAdapter(), [
        data({
          'type': 'response.created',
          'response': {'id': 'resp_1'}
        }, event: 'response.created'),
        data(
            {'type': 'response.reasoning_summary_text.delta', 'delta': 'plan'}),
        data({
          'type': 'response.output_item.added',
          'output_index': 1,
          'item': {
            'type': 'function_call',
            'call_id': 'call_x',
            'name': 'lookup'
          }
        }),
        data({
          'type': 'response.function_call_arguments.delta',
          'output_index': 1,
          'delta': '{"id":'
        }),
        data({
          'type': 'response.function_call_arguments.delta',
          'output_index': 1,
          'delta': '7}'
        }),
        data({
          'type': 'response.function_call_arguments.done',
          'output_index': 1
        }),
        data({'type': 'response.output_text.delta', 'delta': 'Hello '}),
        data({'type': 'response.output_text.delta', 'delta': 'مرحبا'}),
        data({
          'type': 'response.output_text.annotation.added',
          'annotation': {
            'type': 'url_citation',
            'url': 'https://e.com/a',
            'title': 'Doc A'
          }
        }),
        data({
          'type': 'response.completed',
          'response': {
            'usage': {
              'input_tokens': 8,
              'output_tokens': 3,
              'total_tokens': 11,
              'output_tokens_details': {'reasoning_tokens': 2}
            }
          }
        }),
      ]);
      final states = await c.stream(request).toList();
      final snap = states.last.snapshot;
      expect(states.last, isA<AiStreamCompleted>());
      expect(snap.messageId, 'resp_1');
      expect(snap.thinkingText, 'plan');
      expect(snap.text, 'Hello مرحبا');
      expect(snap.citations.single.title, 'Doc A');
      expect(snap.citations.single.urlOrPath, 'https://e.com/a');
      expect(snap.toolBlocks[1]!.id, 'call_x');
      expect(snap.toolBlocks[1]!.isComplete, isTrue);
      expect(snap.toolBlocks[1]!.healedArguments, {'id': 7});
      expect(snap.usage!.reasoningTokens, 2);
      expect(snap.finishReason, 'completed');
      await c.dispose();
    });

    test('response.failed becomes a protocol failure with partial text',
        () async {
      final c = client(const OpenAiResponsesSseAdapter(), [
        data({'type': 'response.output_text.delta', 'delta': 'part'}),
        data({
          'type': 'response.failed',
          'response': {
            'error': {'code': 'server_error', 'message': 'boom'}
          }
        }),
      ]);
      final last = (await c.stream(request).toList()).last as AiStreamFailed;
      expect(last.snapshot.text, 'part');
      expect(last.failure.message, contains('boom'));
      await c.dispose();
    });

    test('response.incomplete completes with the incomplete reason', () async {
      final c = client(const OpenAiResponsesSseAdapter(), [
        data({'type': 'response.output_text.delta', 'delta': 'cut'}),
        data({
          'type': 'response.incomplete',
          'response': {
            'incomplete_details': {'reason': 'max_output_tokens'}
          }
        }),
      ]);
      final last = (await c.stream(request).toList()).last;
      expect(last, isA<AiStreamCompleted>());
      expect(last.snapshot.finishReason, 'max_output_tokens');
      await c.dispose();
    });
  });

  group('CompositeAiAdapter.universal', () {
    test('routes Claude, Responses, Chat and custom RAG frames', () async {
      Future<AiStreamSnapshot> run(List<String> frames) async {
        final c = client(CompositeAiAdapter.universal(), frames);
        final s = (await c.stream(request).toList()).last.snapshot;
        await c.dispose();
        return s;
      }

      expect(
          (await run([
            data({
              'type': 'content_block_delta',
              'index': 0,
              'delta': {'type': 'text_delta', 'text': 'claude'}
            }),
            data({'type': 'message_stop'}),
          ]))
              .text,
          'claude');
      expect(
          (await run([
            data({'type': 'response.output_text.delta', 'delta': 'resp'}),
            data(
                {'type': 'response.completed', 'response': <String, Object>{}}),
          ]))
              .text,
          'resp');
      expect(
          (await run([
            data({
              'choices': [
                {
                  'index': 0,
                  'delta': {'content': 'chat'}
                }
              ]
            }),
            'data: [DONE]\n\n',
          ]))
              .text,
          'chat');
      expect(
          (await run([
            data({'text': 'rag', 'done': true}),
          ]))
              .text,
          'rag');
    });

    test('Claude nested error is not stolen by the Responses adapter',
        () async {
      final c = client(CompositeAiAdapter.universal(), [
        data({
          'type': 'error',
          'error': {'type': 'overloaded_error', 'message': 'busy'}
        }),
      ]);
      final last = (await c.stream(request).toList()).last as AiStreamFailed;
      expect(last.failure.message, contains('overloaded_error'));
      await c.dispose();
    });
  });

  group('Claude usage and localization', () {
    test('usage is merged across message_start and message_delta', () async {
      final c = client(const ClaudeSseAdapter(), [
        data({
          'type': 'message_start',
          'message': {
            'id': 'msg_1',
            'usage': {'input_tokens': 25, 'output_tokens': 1}
          }
        }),
        data({
          'type': 'message_delta',
          'delta': {'stop_reason': 'end_turn'},
          'usage': {'output_tokens': 40}
        }),
        data({'type': 'message_stop'}),
      ]);
      final snap = (await c.stream(request).toList()).last.snapshot;
      expect(snap.usage!.inputTokens, 25);
      expect(snap.usage!.outputTokens, 40);
      expect(snap.usage!.totalTokens, 65);
      expect(snap.finishReason, 'end_turn');
      await c.dispose();
    });

    test('tool status text is configurable (English default, Arabic preset)',
        () async {
      final frames = [
        data({
          'type': 'content_block_start',
          'index': 0,
          'content_block': {'type': 'tool_use', 'id': 't', 'name': 'kb'}
        }),
        data({'type': 'message_stop'}),
      ];
      final en = await client(const ClaudeSseAdapter(), frames)
          .stream(request)
          .toList();
      expect(en.whereType<AiStreamSearchingKnowledgeBase>().first.statusMessage,
          'Calling tool (kb)...');
      final ar = await client(const ClaudeSseAdapter(), frames,
              messages: AiStreamMessages.arabic)
          .stream(request)
          .toList();
      expect(ar.whereType<AiStreamSearchingKnowledgeBase>().first.statusMessage,
          contains('جارٍ'));
    });
  });

  group('Coalescing', () {
    test('frameScheduler aligns emission to the host frame callback', () async {
      final pending = <void Function()>[];
      final seen = <String>[];
      final coalescer = TimerCoalescer<String>(
        interval: const Duration(seconds: 10),
        frameScheduler: pending.add,
        onEmit: seen.add,
      );
      coalescer
        ..schedule('a')
        ..schedule('b')
        ..schedule('c');
      expect(pending.length, 1);
      expect(seen, isEmpty);
      pending.single();
      expect(seen, ['c']);
      coalescer.schedule('d', forceImmediate: true);
      expect(seen, ['c', 'd']);
    });

    test('client emits once per injected frame', () async {
      final pending = <void Function()>[];
      final c = ResilientAiStreamClient(
        transport: CustomCallbackTransport(
            onOpenStream: (_) async => sse([
                  'data: {"text":"a"}\n\n',
                  'data: {"text":"b"}\n\n',
                  'data: [DONE]\n\n',
                ])),
        frameScheduler: (cb) {
          pending.add(cb);
          Future<void>.delayed(const Duration(milliseconds: 5), () {
            if (pending.remove(cb)) cb();
          });
        },
      );
      final states = await c.stream(request).toList();
      expect(states.last, isA<AiStreamCompleted>());
      expect(states.last.snapshot.text, 'ab');
      await c.dispose();
    });
  });
}
