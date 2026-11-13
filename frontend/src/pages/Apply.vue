<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'
const streams = ref([])
const streamId = ref(1)
const last = ref(null)
const runs = ref([])
const err = ref('')
const busy = ref(false)
async function refreshRuns() {
  runs.value = await getJSON('/api/runs')
}
async function runApply() {
  busy.value = true
  err.value = ''
  try {
    last.value = await postJSON('/api/apply', { stream_id: streamId.value })
    streams.value = await getJSON('/api/streams')
    await refreshRuns()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}
onMounted(async () => {
  streams.value = await getJSON('/api/streams')
  if (streams.value.length) streamId.value = streams.value[0].id
  await refreshRuns()
})
</script>
<template>
  <div class="page">
    <h1>应用台</h1>
    <p class="muted">按游标连续序应用；缺口默认停并进隔离区。含 Schema 演化的流会在演化点后按映射规范化再写 Sink。</p>
    <div class="row-actions panel">
      <label>流
        <select v-model.number="streamId">
          <option v-for="s in streams" :key="s.id" :value="s.id">{{ s.id }} · {{ s.name }}（游标 {{ s.apply_cursor }}）</option>
        </select>
      </label>
      <button :disabled="busy" @click="runApply">{{ busy ? '应用中…' : '执行 Apply' }}</button>
    </div>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel" v-if="last">
      <h3>最近一次</h3>
      <p>
        run #{{ last.id }} ·
        <span class="tag" :class="last.status === 'succeeded' ? 'ok' : 'warn'">{{ last.status }}</span>
        · loaded {{ last.loaded_count }} / applied {{ last.applied_count }} /
        quarantined {{ last.quarantined_count }} / skipped {{ last.skipped_count }}
        · cursor {{ last.cursor_before }} → {{ last.cursor_after }}
      </p>
      <div v-if="last.report && last.report.evolution" class="panel" style="margin: 0.5rem 0">
        <h4>Schema 演化对账</h4>
        <div class="tiles">
          <div class="tile"><div class="hero-num">{{ last.report.evolution.pre_evolution_count }}</div><div class="muted">演化前</div></div>
          <div class="tile"><div class="hero-num">{{ last.report.evolution.post_evolution_count }}</div><div class="muted">演化后</div></div>
          <div class="tile"><div class="hero-num">{{ last.report.evolution.mapped_count }}</div><div class="muted">映射成功</div></div>
          <div class="tile"><div class="hero-num">{{ last.report.evolution.mapping_failed_count }}</div><div class="muted">映射失败</div></div>
          <div class="tile"><div class="hero-num">{{ last.applied_count }}</div><div class="muted">最终 applied</div></div>
        </div>
        <p>
          演化点 {{ last.report.evolution.marker_count }} ·
          最终版本 v{{ last.final_schema_version }} ·
          失败策略 {{ last.report.evolution.mapping_failure_policy }} ·
          Sink 版本 <code>{{ JSON.stringify(last.report.evolution.sink_schema_versions) }}</code>
        </p>
        <p>
          <span class="tag" :class="last.report.evolution.consistent ? 'ok' : 'danger'">
            {{ last.report.evolution.consistent ? '对账一致' : '对账不一致' }}
          </span>
        </p>
        <p v-if="last.report.evolution.mapping_failures.length" class="muted">
          映射失败：
          <span v-for="f in last.report.evolution.mapping_failures" :key="f.seq">
            seq {{ f.seq }} {{ f.entity_key }}（缺 {{ f.missing_fields.join('/') }}，{{ f.disposition }}）；
          </span>
        </p>
      </div>
      <pre class="mono">{{ JSON.stringify(last.report, null, 2) }}</pre>
    </div>
    <div class="panel">
      <h3>历史 Runs</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>流</th><th>状态</th><th>loaded</th><th>applied</th><th>quarantine</th><th>skipped</th><th>演化前/后</th><th>映射</th><th>cursor</th><th>时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in runs" :key="r.id">
            <td>{{ r.id }}</td>
            <td>{{ r.stream_id }}</td>
            <td>{{ r.status }}</td>
            <td>{{ r.loaded_count }}</td>
            <td>{{ r.applied_count }}</td>
            <td>{{ r.quarantined_count }}</td>
            <td>{{ r.skipped_count }}</td>
            <td>{{ r.pre_evolution_count }}/{{ r.post_evolution_count }}</td>
            <td>{{ r.mapped_count }}✓ / {{ r.mapping_failed_count }}✗</td>
            <td>{{ r.cursor_before }}→{{ r.cursor_after }}</td>
            <td>{{ r.created_at }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
