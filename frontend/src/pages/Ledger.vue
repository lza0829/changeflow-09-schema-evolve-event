<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const rows = ref([])
onMounted(async () => { rows.value = await getJSON('/api/ledger') })
</script>
<template>
  <div class="page">
    <h1>变更账本</h1>
    <p class="muted">每次 Apply 写入的事件处置记录（与报告同源）。</p>
    <div class="panel">
      <table>
        <thead>
          <tr><th>ID</th><th>流</th><th>run</th><th>seq</th><th>op</th><th>entity</th><th>disposition</th><th>阶段</th><th>版本</th><th>映射</th><th>payload</th><th>event_ts</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in rows" :key="r.id">
            <td>{{ r.id }}</td>
            <td>{{ r.stream_id }}</td>
            <td>{{ r.run_id }}</td>
            <td>{{ r.seq }}</td>
            <td>{{ r.op }}</td>
            <td>{{ r.entity_key }}</td>
            <td>{{ r.disposition }}</td>
            <td>{{ r.evolution_phase || '—' }}</td>
            <td>{{ r.schema_version ? 'v' + r.schema_version : '—' }}</td>
            <td class="muted">
              <span v-if="r.mapping && r.mapping.renamed_fields && r.mapping.renamed_fields.length">
                {{ r.mapping.renamed_fields.join(', ') }}
              </span>
              <span v-else-if="r.mapping && r.mapping.reason">{{ r.mapping.reason }}</span>
              <span v-else>—</span>
            </td>
            <td><pre class="mono">{{ JSON.stringify(r.payload) }}</pre></td>
            <td>{{ r.event_ts }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
