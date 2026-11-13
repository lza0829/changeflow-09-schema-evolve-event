<script setup>
import { onMounted, ref } from 'vue'
import { deleteJSON, getJSON, patchJSON, putJSON } from '../api'
const streams = ref([])
const streamId = ref(1)
const schema = ref(null)
const runs = ref([])
const err = ref('')
const ok = ref('')
const busy = ref(false)

const mapping = ref({ from_version: 1, to_version: 2, fields: [] })
const point = ref({ seq: 1, from_version: 1, to_version: 2, note: '' })

function fieldRow() {
  return { from: '', to: '', required: true }
}

async function loadSchema() {
  err.value = ''
  schema.value = await getJSON(`/api/streams/${streamId.value}/schema`)
  mapping.value = { from_version: 1, to_version: 2, fields: [fieldRow()] }
  const nextSeq = (schema.value.points.at(-1)?.seq || 0) + 1
  const nextVer = schema.value.base_schema_version || 1
  point.value = { seq: nextSeq, from_version: nextVer, to_version: nextVer + 1, note: '' }
}

async function loadAll() {
  streams.value = await getJSON('/api/streams')
  if (streams.value.length && !streams.value.some((s) => s.id === streamId.value)) {
    streamId.value = streams.value[0].id
  }
  await loadSchema()
  const all = await getJSON('/api/runs')
  runs.value = all.filter((r) => r.stream_id === streamId.value)
}

async function saveMapping() {
  err.value = ''
  ok.value = ''
  busy.value = true
  try {
    await putJSON(`/api/streams/${streamId.value}/schema/mappings`, {
      from_version: Number(mapping.value.from_version),
      to_version: Number(mapping.value.to_version),
      fields: mapping.value.fields.map((f) => ({
        from: f.from,
        to: f.to,
        required: Boolean(f.required),
      })),
      note: mapping.value.note || null,
    })
    ok.value = '映射已保存'
    await loadSchema()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function savePoint() {
  err.value = ''
  ok.value = ''
  busy.value = true
  try {
    await putJSON(`/api/streams/${streamId.value}/schema/points`, {
      seq: Number(point.value.seq),
      from_version: Number(point.value.from_version),
      to_version: Number(point.value.to_version),
      note: point.value.note || null,
    })
    ok.value = '演化点已登记'
    await loadSchema()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function removeMapping(m) {
  await deleteJSON(`/api/streams/${streamId.value}/schema/mappings/${m.id}`)
  await loadSchema()
}

function editMapping(m) {
  mapping.value = {
    from_version: m.from_version,
    to_version: m.to_version,
    note: m.note || '',
    fields: m.fields.map((f) => ({ from: f.from, to: f.to, required: Boolean(f.required) })),
  }
}

async function removePoint(p) {
  await deleteJSON(`/api/streams/${streamId.value}/schema/points/${p.id}`)
  await loadSchema()
}

async function setPolicy(policy) {
  await patchJSON(`/api/streams/${streamId.value}`, { mapping_failure_policy: policy })
  await loadSchema()
}

function latestEvolution() {
  const run = runs.value.find((r) => r.report?.evolution)
  return run?.report?.evolution || null
}

onMounted(loadAll)
</script>
<template>
  <div class="page">
    <h1>Schema 演化</h1>
    <p class="muted">
      流内演化：演化点之后的事件先按旧→新字段映射规范化再入账/Apply；演化点之前保持旧形状应用。
      映射缺失关键字段的事件隔离或跳过，绝不静默写入 Sink。
    </p>
    <div class="row-actions panel">
      <label>流
        <select v-model.number="streamId" @change="loadAll">
          <option v-for="s in streams" :key="s.id" :value="s.id">{{ s.id }} · {{ s.name }}</option>
        </select>
      </label>
      <label v-if="schema">映射失败策略
        <select
          :value="schema.mapping_failure_policy"
          @change="setPolicy($event.target.value)"
        >
          <option value="quarantine">隔离（quarantine）</option>
          <option value="skip">跳过（skip）</option>
        </select>
      </label>
    </div>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <p v-if="ok" class="tag ok">{{ ok }}</p>

    <div class="panel" v-if="schema">
      <h3>版本概览</h3>
      <p>
        基础版本 v{{ schema.base_schema_version }} ·
        夹具检测最终 v{{ schema.detected.final_version }} ·
        演化点 {{ schema.points.length }} 个 ·
        映射规则 {{ schema.mappings.length }} 组
      </p>
      <p class="muted">
        夹具中检测到的演化点：<span v-if="!schema.detected.points.length">无</span>
        <span v-for="d in schema.detected.points" :key="d.seq">
          seq {{ d.seq }}（v{{ d.from_version }}→v{{ d.to_version }}，{{ d.source }}）
        </span>
      </p>
    </div>

    <div class="panel" v-if="schema">
      <h3>演化点登记</h3>
      <table>
        <thead><tr><th>ID</th><th>seq</th><th>from</th><th>to</th><th>来源</th><th>备注</th><th></th></tr></thead>
        <tbody>
          <tr v-for="p in schema.points" :key="p.id">
            <td>{{ p.id }}</td>
            <td>{{ p.seq }}</td>
            <td>v{{ p.from_version }}</td>
            <td>v{{ p.to_version }}</td>
            <td><span class="tag" :class="p.source === 'manual' ? 'warn' : 'ok'">{{ p.source }}</span></td>
            <td class="muted">{{ p.note || '—' }}</td>
            <td class="row-actions">
              <button class="ghost" @click="point = { seq: p.seq, from_version: p.from_version, to_version: p.to_version, note: p.note || '' }">到编辑区</button>
              <button class="ghost" @click="removePoint(p)">删除</button>
            </td>
          </tr>
          <tr v-if="!schema.points.length"><td colspan="7" class="muted">暂无演化点</td></tr>
        </tbody>
      </table>
      <div class="row-actions" style="margin-top: 0.75rem">
        <label>seq <input v-model.number="point.seq" type="number" min="1" style="width: 5rem" /></label>
        <label>from v<input v-model.number="point.from_version" type="number" min="1" style="width: 4rem" /></label>
        <label>to v<input v-model.number="point.to_version" type="number" min="1" style="width: 4rem" /></label>
        <label>备注 <input v-model="point.note" placeholder="可选" /></label>
        <button :disabled="busy" @click="savePoint">登记演化点</button>
      </div>
    </div>

    <div class="panel" v-if="schema">
      <h3>字段映射规则</h3>
      <table>
        <thead><tr><th>ID</th><th>版本</th><th>字段映射</th><th>备注</th><th></th></tr></thead>
        <tbody>
          <tr v-for="m in schema.mappings" :key="m.id">
            <td>{{ m.id }}</td>
            <td>v{{ m.from_version }} → v{{ m.to_version }}</td>
            <td>
              <div v-for="f in m.fields" :key="f.to">
                <code>{{ f.from }} → {{ f.to }}</code>
                <span v-if="f.required" class="tag warn">必需</span>
              </div>
            </td>
            <td class="muted">{{ m.note || '—' }}</td>
            <td class="row-actions">
              <button class="ghost" @click="editMapping(m)">编辑</button>
              <button class="ghost" @click="removeMapping(m)">删除</button>
            </td>
          </tr>
          <tr v-if="!schema.mappings.length"><td colspan="5" class="muted">暂无映射规则</td></tr>
        </tbody>
      </table>
      <div style="margin-top: 0.75rem">
        <div class="row-actions">
          <label>from v<input v-model.number="mapping.from_version" type="number" min="1" style="width: 4rem" /></label>
          <label>to v<input v-model.number="mapping.to_version" type="number" min="1" style="width: 4rem" /></label>
          <label>备注 <input v-model="mapping.note" placeholder="可选" /></label>
        </div>
        <div class="row-actions" style="margin-top: 0.5rem" v-for="(f, i) in mapping.fields" :key="i">
          <label>旧字段 <input v-model="f.from" placeholder="order_status" /></label>
          <label>新字段 <input v-model="f.to" placeholder="status" /></label>
          <label>必需 <input v-model="f.required" type="checkbox" /></label>
          <button class="ghost" @click="mapping.fields.splice(i, 1)">移除</button>
        </div>
        <div class="row-actions" style="margin-top: 0.5rem">
          <button class="ghost" @click="mapping.fields.push(fieldRow())">+ 字段</button>
          <button :disabled="busy" @click="saveMapping">保存映射</button>
        </div>
      </div>
    </div>

    <div class="panel" v-if="latestEvolution()">
      <h3>最近一次演化结果（run #{{ runs.find((r) => r.report?.evolution)?.id }}）</h3>
      <div class="tiles">
        <div class="tile"><div class="hero-num">{{ latestEvolution().pre_evolution_count }}</div><div class="muted">演化前条数</div></div>
        <div class="tile"><div class="hero-num">{{ latestEvolution().post_evolution_count }}</div><div class="muted">演化后条数</div></div>
        <div class="tile"><div class="hero-num">{{ latestEvolution().mapped_count }}</div><div class="muted">映射成功</div></div>
        <div class="tile"><div class="hero-num">{{ latestEvolution().mapping_failed_count }}</div><div class="muted">映射失败</div></div>
        <div class="tile"><div class="hero-num">{{ runs.find((r) => r.report?.evolution)?.applied_count }}</div><div class="muted">最终 applied</div></div>
      </div>
      <p>
        演化点 {{ latestEvolution().marker_count }} ·
        映射失败策略 {{ latestEvolution().mapping_failure_policy }} ·
        Sink 版本分布
        <code>{{ JSON.stringify(latestEvolution().sink_schema_versions) }}</code>
      </p>
      <p>
        <span class="tag" :class="latestEvolution().consistent ? 'ok' : 'danger'">
          {{ latestEvolution().consistent ? '账本/隔离/Sink 对账一致' : '对账不一致' }}
        </span>
        <span v-if="latestEvolution().inconsistencies.length" class="muted">
          {{ latestEvolution().inconsistencies.join('；') }}
        </span>
      </p>
      <h4>映射失败清单</h4>
      <table>
        <thead><tr><th>seq</th><th>entity</th><th>原因</th><th>缺失字段</th><th>处置</th></tr></thead>
        <tbody>
          <tr v-for="f in latestEvolution().mapping_failures" :key="f.seq">
            <td>{{ f.seq }}</td>
            <td>{{ f.entity_key }}</td>
            <td>{{ f.reason }}</td>
            <td>{{ f.missing_fields.join(', ') || '—' }}</td>
            <td>{{ f.disposition }}</td>
          </tr>
          <tr v-if="!latestEvolution().mapping_failures.length"><td colspan="5" class="muted">无映射失败</td></tr>
        </tbody>
      </table>
      <h4>规则配置（本次使用的映射）</h4>
      <table>
        <thead><tr><th>版本</th><th>已配置</th><th>字段</th></tr></thead>
        <tbody>
          <tr v-for="r in latestEvolution().mapping_rules" :key="r.to_version">
            <td>v{{ r.from_version }} → v{{ r.to_version }}</td>
            <td><span class="tag" :class="r.configured ? 'ok' : 'danger'">{{ r.configured ? '是' : '否' }}</span></td>
            <td>
              <code v-for="f in r.fields" :key="f.to">{{ f.from }}→{{ f.to }}{{ f.required ? '*' : '' }}&nbsp;</code>
              <span v-if="!r.fields.length" class="muted">—</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>历史 Runs（本流）</h3>
      <table>
        <thead><tr><th>ID</th><th>状态</th><th>applied</th><th>quarantine</th><th>skipped</th><th>演化前</th><th>演化后</th><th>映射成功</th><th>映射失败</th><th>最终版本</th></tr></thead>
        <tbody>
          <tr v-for="r in runs" :key="r.id">
            <td>{{ r.id }}</td>
            <td>{{ r.status }}</td>
            <td>{{ r.applied_count }}</td>
            <td>{{ r.quarantined_count }}</td>
            <td>{{ r.skipped_count }}</td>
            <td>{{ r.pre_evolution_count }}</td>
            <td>{{ r.post_evolution_count }}</td>
            <td>{{ r.mapped_count }}</td>
            <td>{{ r.mapping_failed_count }}</td>
            <td>v{{ r.final_schema_version }}</td>
          </tr>
          <tr v-if="!runs.length"><td colspan="10" class="muted">暂无 Run</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
