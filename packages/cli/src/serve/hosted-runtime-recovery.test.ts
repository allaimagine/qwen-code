/**
 * @license
 * Copyright 2026 Qwen Team
 * SPDX-License-Identifier: Apache-2.0
 */

import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { LocalJsonlManagedSessionJournalStore } from '@qwen-code/qwen-code-core/managed-runtime/local-jsonl-managed-session-journal-store.js';
import { LocalManagedSessionResourceStore } from '@qwen-code/qwen-code-core/managed-runtime/managed-session-resources.js';
import {
  openManagedSession,
  type ManagedSession,
} from '@qwen-code/qwen-code-core/managed-runtime/managed-session-assembly.js';
import { createManagedHarnessHandle } from '@qwen-code/qwen-code-core/managed-runtime/managed-harness-factory.js';
import { resetManagedRuntimeDispatchGatesForTest } from '@qwen-code/qwen-code-core/managed-runtime/managed-runtime-dispatch-gate.js';
import {
  stopParkedRuntimeExecutions,
  recoverHostedRuntimeTurn,
} from './hosted-runtime-recovery.js';
import { HostedWorkspaceBroker } from './hosted-workspace-broker.js';

const SESSION_ID = '22222222-2222-4222-8222-222222222222';
const PROMPT_ID = '33333333-3333-4333-8333-333333333333';
const EXECUTION_ID = 'exec-1';
const DIGEST = 'a'.repeat(64);

describe('recoverHostedRuntimeTurn', () => {
  let root: string;

  beforeEach(async () => {
    resetManagedRuntimeDispatchGatesForTest();
    root = await mkdtemp(path.join(tmpdir(), 'hosted-recovery-test-'));
  });
  afterEach(async () => {
    vi.restoreAllMocks();
    await rm(root, { recursive: true, force: true });
  });

  async function open(workerId: string, create: boolean) {
    const sessionKey = {
      tenantId: 'tenant',
      workspaceId: 'workspace',
      sessionId: SESSION_ID,
    };
    const resourceStore = LocalManagedSessionResourceStore.create({
      runtimeBaseDir: root,
      sessionKey,
    });
    const refs = create
      ? {
          definitionRef: await resourceStore.publish(
            'managed-definition',
            Buffer.from(
              JSON.stringify({
                engine: 'managed',
                sessionId: SESSION_ID,
                toolProfile: 'hosted-workspace-files/1',
              }),
            ),
          ),
          rootSnapshotRef: await resourceStore.publish(
            'managed-root',
            Buffer.from(JSON.stringify({ cwd: root })),
          ),
          createdBy: 'hosted-harness',
        }
      : undefined;
    return openManagedSession({
      runtimeBaseDir: root,
      transcriptPath: '',
      sessionId: SESSION_ID,
      sessionKey,
      cwd: root,
      version: 'hosted-harness/1',
      workerId,
      activationLeaseDurationMs: 60_000,
      journalStore: new LocalJsonlManagedSessionJournalStore({
        runtimeBaseDir: root,
        sessionId: SESSION_ID,
        transcriptPath: path.join(root, `${SESSION_ID}.jsonl`),
      }),
      resourceStore,
      ...(refs ? { create: refs, requireNew: true } : {}),
    });
  }

  /** Drives a fresh session to a parked await_runtime checkpoint. */
  async function parkAtAwaitRuntime(
    toolName = 'write_file',
    preJournalResult = false,
  ): Promise<ManagedSession> {
    const session = await open('boot-1', true);
    const harness = createManagedHarnessHandle(session);
    const authority = session.authority;
    const contentRef = await session.resources.publish(
      'managed-input',
      Buffer.from(JSON.stringify([{ type: 'text', text: 'write a file' }])),
    );
    const admissionRef = await session.resources.publish(
      'managed-admission',
      Buffer.from(JSON.stringify({ promptId: PROMPT_ID, digest: 'x' })),
    );
    await authority.submitInput(
      {
        operation: 'submitInput',
        commandId: PROMPT_ID,
        sessionKey: authority.sessionHeader.sessionKey,
        contentDigest: DIGEST,
      },
      {
        inputId: PROMPT_ID,
        turnId: PROMPT_ID,
        source: 'hosted-harness',
        contentRef,
        admissionRef,
        deadline: null,
        wakeReason: 'input',
      },
    );
    await harness.ensureRunnable();
    const inputBytes = Buffer.from(
      JSON.stringify({
        harnessSessionId: SESSION_ID,
        runtimeSessionId: PROMPT_ID,
        payloadJson: JSON.stringify({
          toolName,
          input: { file_path: 'a.txt', content: 'x' },
        }),
      }),
    );
    const routeRef = await session.resources.publish(
      'managed-tool-input',
      inputBytes,
    );
    const definitionRef = await session.resources.publish(
      'managed-tool-definition',
      Buffer.from(JSON.stringify({ name: toolName })),
    );
    const activation = session.activation;
    await authority.appendExecutionEvent(
      {
        operation: 'toolIntent',
        commandId: `tool-intent:${EXECUTION_ID}`,
        sessionKey: authority.sessionHeader.sessionKey,
        contentDigest: routeRef.digest,
      },
      (sequence) => ({
        v: 1,
        sequence,
        eventId: `tool-intent:${EXECUTION_ID}`,
        sessionKey: authority.sessionHeader.sessionKey,
        kind: 'tool.intent',
        occurredAt: Date.now(),
        subject: {
          type: 'activation',
          scopeId: activation.activationId,
          ...activation,
        },
        payload: {
          executionCallId: EXECUTION_ID,
          batchId: 'batch-1',
          ordinal: 0,
          toolDefinitionRef: definitionRef,
          argsRef: routeRef,
          outcomeSource: 'runtime',
        },
      }),
      { class: 'harness', activation },
    );
    await harness.commitAwaitRuntimeBatch(
      [
        {
          functionCallId: 'call-1',
          toolName,
          executionCallId: EXECUTION_ID,
          invocationBindingId: EXECUTION_ID,
          capabilityVersion: 'workspace-capability/1',
          policyVersion: 'preapproved-workspace-tools/1',
          mediaVersion: null,
          modelMessageId: 'message-1',
          partIndex: 0,
          ordinal: 0,
          inputDigest: DIGEST,
          progressCursor: null,
          attemptId: 'attempt-1',
          routeRef,
        },
      ],
      { turnId: PROMPT_ID, promptId: PROMPT_ID },
    );
    if (preJournalResult) {
      await session.sink.write({
        uuid: 'result-1',
        parentUuid: 'message-1',
        sessionId: SESSION_ID,
        timestamp: new Date().toISOString(),
        type: 'tool_result',
        cwd: root,
        version: 'hosted-harness/1',
        daemonPromptId: PROMPT_ID,
        message: {
          role: 'user',
          parts: [
            {
              functionResponse: {
                id: 'call-1',
                name: toolName,
                response: { executionStatus: 'success' },
              },
            },
          ],
        },
      });
    }
    await session.close();
    resetManagedRuntimeDispatchGatesForTest();
    return session;
  }

  const brokerOptions = { baseUrl: 'http://127.0.0.1:1', token: 'test' };

  it('settles parked executions under their original ids and reports ready', async () => {
    await parkAtAwaitRuntime();
    const acquire = vi
      .spyOn(HostedWorkspaceBroker.prototype, 'acquire')
      .mockResolvedValue();
    const execute = vi
      .spyOn(HostedWorkspaceBroker.prototype, 'execute')
      .mockResolvedValue({
        executionStatus: 'success',
        responseParts: [{ text: 'done' }],
      } as never);
    const replacement = await open('boot-2', false);
    try {
      const recovered = await recoverHostedRuntimeTurn({
        session: replacement,
        sessionId: SESSION_ID,
        cwd: root,
        promptId: PROMPT_ID,
        brokerOptions,
        passive: false,
      });
      expect(recovered).toBeDefined();
      expect(execute).toHaveBeenCalledOnce();
      expect(execute.mock.calls[0]?.[0]).toBe(EXECUTION_ID);
      expect(recovered!.report).toMatchObject({
        phase: 'results_ready',
        checkpointId: replacement.authority.latestCheckpoint?.checkpointId,
        activationId: replacement.activation.activationId,
        continuationAdmitted: false,
        executions: [
          {
            functionCallId: 'call-1',
            toolName: 'write_file',
            executionCallId: EXECUTION_ID,
            runtimeSessionId: PROMPT_ID,
            outcome: 'known',
            status: { state: 'settled' },
          },
        ],
      });
      const authorization =
        await replacement.authority.harnessRunAuthorization();
      expect(authorization.status).toBe('runnable');
      if (authorization.status === 'runnable') {
        expect(authorization.checkpoint.continuation.phase).toBe(
          'results_ready',
        );
      }
      const projected = await replacement.sink.project();
      const toolResult = projected.find(
        (entry) =>
          entry.daemonPromptId === PROMPT_ID && entry.type === 'tool_result',
      );
      expect(toolResult).toBeDefined();
    } finally {
      await replacement.close();
    }
    expect(acquire).toHaveBeenCalled();
  });

  it('reports parked executions without dispatching on a passive load', async () => {
    await parkAtAwaitRuntime();
    const execute = vi
      .spyOn(HostedWorkspaceBroker.prototype, 'execute')
      .mockRejectedValue(new Error('must not dispatch'));
    vi.spyOn(HostedWorkspaceBroker.prototype, 'status').mockResolvedValue({
      state: 'prepared',
    });
    const replacement = await open('boot-2', false);
    try {
      const recovered = await recoverHostedRuntimeTurn({
        session: replacement,
        sessionId: SESSION_ID,
        cwd: root,
        promptId: PROMPT_ID,
        brokerOptions,
        passive: true,
      });
      expect(recovered).toBeDefined();
      expect(execute).not.toHaveBeenCalled();
      expect(recovered!.report).toMatchObject({
        phase: 'await_runtime',
        executions: [
          {
            executionCallId: EXECUTION_ID,
            outcome: 'known',
            status: { state: 'prepared' },
          },
        ],
      });
      const authorization =
        await replacement.authority.harnessRunAuthorization();
      expect(authorization.status).toBe('runnable');
      if (authorization.status === 'runnable') {
        expect(authorization.checkpoint.continuation.phase).toBe(
          'await_runtime',
        );
      }
    } finally {
      await replacement.close();
    }
  });

  it('reports an execution the Broker cannot account for as unknown', async () => {
    await parkAtAwaitRuntime();
    vi.spyOn(HostedWorkspaceBroker.prototype, 'status').mockResolvedValue(
      undefined,
    );
    const replacement = await open('boot-2', false);
    try {
      const recovered = await recoverHostedRuntimeTurn({
        session: replacement,
        sessionId: SESSION_ID,
        cwd: root,
        promptId: PROMPT_ID,
        brokerOptions,
        passive: true,
      });
      expect(recovered!.report.executions).toEqual([
        expect.objectContaining({
          executionCallId: EXECUTION_ID,
          outcome: 'unknown',
        }),
      ]);
    } finally {
      await replacement.close();
    }
  });

  it('refuses a turn whose checkpoint is not a Runtime wait', async () => {
    const session = await open('boot-1', true);
    try {
      const recovered = await recoverHostedRuntimeTurn({
        session,
        sessionId: SESSION_ID,
        cwd: root,
        promptId: PROMPT_ID,
        brokerOptions,
        passive: false,
      });
      expect(recovered).toBeUndefined();
    } finally {
      await session.close();
    }
  });

  it('cancels parked executions without settling them', async () => {
    await parkAtAwaitRuntime();
    let stopped = false;
    vi.spyOn(HostedWorkspaceBroker.prototype, 'status').mockImplementation(
      async () => ({ state: stopped ? 'settled' : 'executing' }),
    );
    const cancel = vi
      .spyOn(HostedWorkspaceBroker.prototype, 'cancel')
      .mockImplementation(async () => {
        stopped = true;
      });
    const replacement = await open('boot-2', false);
    try {
      await stopParkedRuntimeExecutions({
        session: replacement,
        promptId: PROMPT_ID,
        brokerOptions,
      });
      expect(cancel).toHaveBeenCalledOnce();
      expect(cancel).toHaveBeenCalledWith(EXECUTION_ID);
    } finally {
      await replacement.close();
    }
  });

  it('refuses to re-dispatch a parked Shell execution on a continuation load', async () => {
    await parkAtAwaitRuntime('run_shell_command');
    const acquire = vi.spyOn(HostedWorkspaceBroker.prototype, 'acquire');
    const execute = vi.spyOn(HostedWorkspaceBroker.prototype, 'execute');
    const replacement = await open('boot-2', false);
    try {
      const recovered = await recoverHostedRuntimeTurn({
        session: replacement,
        sessionId: SESSION_ID,
        cwd: root,
        promptId: PROMPT_ID,
        brokerOptions,
        passive: false,
      });
      expect(recovered).toBeUndefined();
      expect(acquire).not.toHaveBeenCalled();
      expect(execute).not.toHaveBeenCalled();
    } finally {
      await replacement.close();
    }
  });

  it('omits an oversized settled output instead of failing the load', async () => {
    await parkAtAwaitRuntime();
    vi.spyOn(HostedWorkspaceBroker.prototype, 'acquire').mockResolvedValue();
    vi.spyOn(HostedWorkspaceBroker.prototype, 'execute').mockResolvedValue({
      executionStatus: 'success',
      responseParts: [{ text: 'x'.repeat(2 * 1024 * 1024) }],
    } as never);
    const replacement = await open('boot-2', false);
    try {
      const recovered = await recoverHostedRuntimeTurn({
        session: replacement,
        sessionId: SESSION_ID,
        cwd: root,
        promptId: PROMPT_ID,
        brokerOptions,
        passive: false,
      });
      expect(recovered).toBeDefined();
      expect(recovered!.report.phase).toBe('results_ready');
      const projected = await replacement.sink.project();
      const result = projected.find(
        (entry) =>
          entry.daemonPromptId === PROMPT_ID && entry.type === 'tool_result',
      );
      expect(JSON.stringify(result)).toContain('outputOmitted');
    } finally {
      await replacement.close();
    }
  });

  it('does not journal a tool result twice across a recovery retry', async () => {
    await parkAtAwaitRuntime('write_file', true);
    vi.spyOn(HostedWorkspaceBroker.prototype, 'acquire').mockResolvedValue();
    vi.spyOn(HostedWorkspaceBroker.prototype, 'execute').mockResolvedValue({
      executionStatus: 'success',
      responseParts: [{ text: 'done' }],
    } as never);
    const replacement = await open('boot-2', false);
    try {
      const recovered = await recoverHostedRuntimeTurn({
        session: replacement,
        sessionId: SESSION_ID,
        cwd: root,
        promptId: PROMPT_ID,
        brokerOptions,
        passive: false,
      });
      expect(recovered).toBeDefined();
      const results = (await replacement.sink.project()).filter(
        (entry) =>
          entry.daemonPromptId === PROMPT_ID && entry.type === 'tool_result',
      );
      expect(results).toHaveLength(1);
    } finally {
      await replacement.close();
    }
  });

  it('reports only the state, never the result payload, from a passive read', async () => {
    await parkAtAwaitRuntime();
    vi.spyOn(HostedWorkspaceBroker.prototype, 'status').mockResolvedValue({
      state: 'settled',
      result: { executionStatus: 'success', responseParts: [{ text: 'big' }] },
    });
    const replacement = await open('boot-2', false);
    try {
      const recovered = await recoverHostedRuntimeTurn({
        session: replacement,
        sessionId: SESSION_ID,
        cwd: root,
        promptId: PROMPT_ID,
        brokerOptions,
        passive: true,
      });
      expect(recovered!.report.executions[0]).toEqual({
        functionCallId: 'call-1',
        toolName: 'write_file',
        executionCallId: EXECUTION_ID,
        runtimeSessionId: PROMPT_ID,
        outcome: 'known',
        status: { state: 'settled' },
      });
    } finally {
      await replacement.close();
    }
  });
});
