<script setup>
import { ref, reactive, onMounted, computed } from 'vue'
import { api, session, fmtNum, fmtTime, UnauthorizedError } from '../api.js'

// 管理后台：API Key 管理 / 渠道健康 / 审计日志 / 调试沙盒
const props = defineProps({ customers: { type: Array, default: () => [] } })
const emit = defineEmits(['expired'])

const tabs = [
  { key: 'keys', label: 'API Key 管理' },
  { key: 'health', label: '渠道健康' },
  { key: 'audit', label: '审计日志' },
  { key: 'debug', label: '调试沙盒' },
]
const tab = ref('keys')

function onErr(e) {
  if (e instanceof UnauthorizedError) emit('expired')
  return e.message
}

/* ---------- API Key 管理 ---------- */

const keys = ref([])
const keysErr = ref('')
const keysLoading = ref(false)

const newKey = reactive({
  customer_id: '', scopes: 'admin,gateway_invoke', expire_at: '',
  ip_whitelist: '', model_allow_list: '',
})
const creating = ref(false)
const created = ref(null) // { api_key, id } 明文仅展示一次
const createErr = ref('')

async function loadKeys() {
  keysLoading.value = true
  keysErr.value = ''
  try {
    keys.value = await api('/api/v1/api-keys')
  } catch (e) {
    keysErr.value = onErr(e)
  } finally {
    keysLoading.value = false
  }
}

async function createKey() {
  creating.value = true
  createErr.value = ''
  created.value = null
  try {
    const body = {
      scopes: newKey.scopes.split(',').map((s) => s.trim()).filter(Boolean),
      ip_whitelist: newKey.ip_whitelist.split(',').map((s) => s.trim()).filter(Boolean),
      model_allow_list: newKey.model_allow_list.split(',').map((s) => s.trim()).filter(Boolean),
    }
    if (newKey.customer_id) body.customer_id = newKey.customer_id
    if (newKey.expire_at) body.expire_at = new Date(newKey.expire_at).toISOString()
    created.value = await api('/api/v1/api-keys', {
      method: 'POST',
      body: JSON.stringify(body),
    })
    await loadKeys()
  } catch (e) {
    createErr.value = onErr(e)
  } finally {
    creating.value = false
  }
}

async function toggleKey(k) {
  try {
    await api(`/api/v1/api-keys/${k.id}/enabled`, {
      method: 'PATCH',
      body: JSON.stringify({ enabled: !k.is_enabled }),
    })
    await loadKeys()
  } catch (e) { keysErr.value = onErr(e) }
}

async function deleteKey(k) {
  if (!confirm('确认删除该密钥？删除后立即失效（软删除可审计）')) return
  try {
    await api(`/api/v1/api-keys/${k.id}`, { method: 'DELETE' })
    await loadKeys()
  } catch (e) { keysErr.value = onErr(e) }
}

const customerName = computed(() => {
  const m = new Map(props.customers.map((c) => [c.id, c.name]))
  return (id) => (id ? (m.get(id) || id.slice(0, 8)) : '平台级')
})

/* ---------- 渠道健康 ---------- */

const healthList = ref([])
const healthErr = ref('')
const healthLoading = ref(false)

async function loadHealth() {
  healthLoading.value = true
  healthErr.value = ''
  try {
    const snap = await api('/api/v1/providers/health')
    healthList.value = Array.isArray(snap) ? snap : []
  } catch (e) {
    healthErr.value = onErr(e)
  } finally {
    healthLoading.value = false
  }
}

async function setProvider(name, enabled) {
  try {
    await api(`/api/v1/providers/${encodeURIComponent(name)}/${enabled ? 'enable' : 'disable'}`, { method: 'POST' })
    await loadHealth()
  } catch (e) { healthErr.value = onErr(e) }
}

function errRate(h) {
  if (!h.total) return '0%'
  return (h.error_rate != null ? (h.error_rate * 100).toFixed(1) : '0') + '%'
}

/* ---------- 审计日志 ---------- */

const auditFilter = reactive({
  customer_id: '', task_type: '', agent_id: '', model: '', provider: '',
  status_code: '', hours: 24, limit: 100, offset: 0,
})
const auditEvents = ref([])
const auditErr = ref('')
const auditLoading = ref(false)

async function loadAudit() {
  auditLoading.value = true
  auditErr.value = ''
  try {
    const q = new URLSearchParams()
    Object.entries(auditFilter).forEach(([k, v]) => {
      if (v !== '' && v != null) q.set(k, String(v))
    })
    if (auditFilter.hours) {
      const now = new Date()
      q.set('from', new Date(now.getTime() - auditFilter.hours * 3600e3).toISOString())
      q.set('to', now.toISOString())
    }
    const res = await api('/api/v1/audit/events?' + q.toString())
    auditEvents.value = res.events || []
  } catch (e) {
    auditErr.value = onErr(e)
  } finally {
    auditLoading.value = false
  }
}

function statusBadge(code) {
  if (!code) return ''
  if (code < 300) return 'finalized'
  if (code < 500) return 'warn'
  return 'danger'
}

function turnPage(delta) {
  auditFilter.offset = Math.max(0, auditFilter.offset + delta)
  loadAudit()
}

function searchAudit() {
  auditFilter.offset = 0
  loadAudit()
}

/* ---------- 调试沙盒 ---------- */

const debugForm = reactive({
  model: '', customer_id: '', task_type: 'chat', agent_id: 'debug-sandbox', message: '',
  max_tokens: 256,
})
const debugResult = ref('') // 响应体原文（JSON/SSE）
const debugErr = ref('')
const debugStatus = ref(0)
const debugMs = ref(0)
const debugging = ref(false)

const modelsCache = ref([])

onMounted(async () => {
  loadKeys()
  loadHealth()
  try {
    // 复用模型列表（/api/v1/models）
    const models = await api('/api/v1/models')
    modelsCache.value = models.map((m) => m.model)
    if (models.length) debugForm.model = models[0].model
  } catch { /* 手输兜底 */ }
})

async function copyResult() {
  if (!debugResult.value) return
  try { await navigator.clipboard.writeText(debugResult.value) } catch { /* 忽略复制失败 */ }
}

async function runDebug() {
  if (!debugForm.message.trim() || !debugForm.model) {
    debugErr.value = '请填写模型与消息'
    return
  }
  debugging.value = true
  debugErr.value = ''
  debugResult.value = ''
  const t0 = performance.now()
  try {
    const body = {
      model: debugForm.model,
      messages: [{ role: 'user', content: debugForm.message }],
      max_tokens: Number(debugForm.max_tokens) || 256,
    }
    const headers = {
      'X-Meteron-Task-Type': debugForm.task_type || 'chat',
      'X-Meteron-Agent-ID': debugForm.agent_id || 'debug-sandbox',
    }
    if (debugForm.customer_id) headers['X-Meteron-Customer-ID'] = debugForm.customer_id
    const res = await fetch('/api/v1/debug/chat', {
      method: 'POST',
      headers: {
        Authorization: 'Bearer ' + session.key,
        'Content-Type': 'application/json',
        ...headers,
      },
      body: JSON.stringify(body),
    })
    debugStatus.value = res.status
    debugMs.value = Math.round(performance.now() - t0)
    const text = await res.text()
    debugResult.value = text
    if (!res.ok) {
      // 尝试解析错误信封
      try { debugErr.value = JSON.parse(text).msg || ('HTTP ' + res.status) }
      catch { debugErr.value = 'HTTP ' + res.status }
    }
  } catch (e) {
    debugErr.value = e.message
  } finally {
    debugging.value = false
  }
}
</script>

<template>
  <div class="view">
    <!-- 标签页 -->
    <div class="range admin-tabs">
      <button v-for="t in tabs" :key="t.key" :class="{ active: tab === t.key }" @click="tab = t.key">
        {{ t.label }}
      </button>
    </div>

    <!-- API Key 管理 -->
    <div v-if="tab === 'keys'" class="grid-2">
      <div class="card">
        <div class="card-head"><h2>密钥列表</h2>
          <button class="btn" @click="loadKeys" :disabled="keysLoading">{{ keysLoading ? '加载中…' : '刷新' }}</button>
        </div>
        <p v-if="keysErr" class="chat-err">{{ keysErr }}</p>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>密钥</th><th>绑定对象</th><th>scope</th><th>白名单</th>
                <th>过期时间</th><th>状态</th><th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="!keys.length && !keysLoading">
                <td colspan="7" class="empty">暂无密钥（左侧创建）</td>
              </tr>
              <tr v-for="k in keys" :key="k.id">
                <td :title="k.id">{{ (k.id || '').slice(0, 8) }}…</td>
                <td>{{ customerName(k.customer_id) }}</td>
                <td class="small">{{ (k.scopes || []).join(',') }}</td>
                <td class="small">
                  <span v-if="k.ip_whitelist?.length" :title="'IP: ' + k.ip_whitelist.join(',')">
                    IP×{{ k.ip_whitelist.length }}
                  </span>
                  <span v-if="k.model_allow_list?.length" :title="'模型: ' + k.model_allow_list.join(',')">
                    模型×{{ k.model_allow_list.length }}
                  </span>
                  <span v-if="!k.ip_whitelist?.length && !k.model_allow_list?.length" class="muted">无限制</span>
                </td>
                <td class="small">{{ k.expire_at ? fmtTime(k.expire_at) : '永久' }}</td>
                <td>
                  <span class="badge" :class="k.is_enabled ? 'finalized' : 'danger'">
                    {{ k.is_enabled ? '启用' : '禁用' }}
                  </span>
                </td>
                <td>
                  <button class="btn small-btn" @click="toggleKey(k)">{{ k.is_enabled ? '禁用' : '启用' }}</button>
                  <button class="btn small-btn danger-btn" @click="deleteKey(k)">删除</button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <div class="card-head"><h2>创建密钥</h2></div>
        <div class="form-col">
          <label class="ctl">绑定客户（平台级密钥可留空）
            <select v-model="newKey.customer_id">
              <option value="">（平台级：仅 tenant 维度）</option>
              <option v-for="c in customers" :key="c.id" :value="c.id">{{ c.name }}</option>
            </select>
          </label>
          <label class="ctl">scope（逗号分隔，默认 admin,gateway_invoke）
            <input v-model="newKey.scopes" placeholder="admin,gateway_invoke">
          </label>
          <label class="ctl">过期时间（留空=永久）
            <input v-model="newKey.expire_at" type="datetime-local">
          </label>
          <label class="ctl">IP 白名单（逗号分隔 CIDR/IP，空=不限）
            <input v-model="newKey.ip_whitelist" placeholder="10.0.0.0/8, 192.168.1.5">
          </label>
          <label class="ctl">模型白名单（逗号分隔，空=允许全部）
            <input v-model="newKey.model_allow_list" placeholder="mock-chat, qwen3.5:9b">
          </label>
          <button class="btn primary" :disabled="creating" @click="createKey">
            {{ creating ? '创建中…' : '创建密钥' }}
          </button>
          <p v-if="createErr" class="chat-err">{{ createErr }}</p>
          <div v-if="created" class="created-key">
            <p class="small">新密钥明文（仅此一次展示，请立即保存）：</p>
            <code class="key-plaintext">{{ created.api_key }}</code>
          </div>
        </div>
      </div>
    </div>

    <!-- 渠道健康 -->
    <div v-else-if="tab === 'health'" class="card">
      <div class="card-head"><h2>Provider 渠道健康</h2>
        <button class="btn" @click="loadHealth" :disabled="healthLoading">{{ healthLoading ? '加载中…' : '刷新' }}</button>
      </div>
      <p v-if="healthErr" class="chat-err">{{ healthErr }}</p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>渠道</th><th class="num">样本数</th><th class="num">错误数</th><th class="num">错误率</th>
              <th class="num">平均延迟</th><th>状态</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="!healthList.length && !healthLoading">
              <td colspan="7" class="empty">暂无数据（需有流量或开启主动探测）</td>
            </tr>
            <tr v-for="h in healthList" :key="h.provider">
              <td>{{ h.provider }}</td>
              <td class="num">{{ fmtNum(h.total) }}</td>
              <td class="num">{{ fmtNum(h.errors) }}</td>
              <td class="num" :class="{ 'err-text': (h.error_rate || 0) > 0.3 }">{{ errRate(h) }}</td>
              <td class="num">{{ h.avg_latency_ms != null ? Math.round(h.avg_latency_ms) + 'ms' : '-' }}</td>
              <td>
                <span class="badge"
                  :class="h.disabled ? 'danger' : ((h.error_rate || 0) > 0.3 ? 'warn' : 'finalized')">
                  {{ h.disabled ? (h.auto_disabled ? '自动降级' : '人工禁用') : '可用' }}
                </span>
              </td>
              <td>
                <button v-if="h.disabled" class="btn small-btn" @click="setProvider(h.provider, true)">恢复</button>
                <button v-else class="btn small-btn danger-btn" @click="setProvider(h.provider, false)">禁用</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p class="muted small" style="margin-top: 10px">
        错误率超阈值自动降级（路由引擎跳过该渠道）；恢复将重置统计从零观察。指标同时暴露于 Prometheus（meteron_provider_health_*）。
      </p>
    </div>

    <!-- 审计日志 -->
    <div v-else-if="tab === 'audit'" class="card">
      <div class="card-head"><h2>请求审计日志（ClickHouse meter_events）</h2></div>
      <div class="audit-filter">
        <label class="ctl">客户
          <select v-model="auditFilter.customer_id">
            <option value="">全部</option>
            <option v-for="c in customers" :key="c.id" :value="c.id">{{ c.name }}</option>
          </select>
        </label>
        <label class="ctl">模型 <input v-model="auditFilter.model" placeholder="model"></label>
        <label class="ctl">任务类型 <input v-model="auditFilter.task_type" placeholder="task_type"></label>
        <label class="ctl">Agent <input v-model="auditFilter.agent_id" placeholder="agent_id"></label>
        <label class="ctl">Provider <input v-model="auditFilter.provider" placeholder="provider"></label>
        <label class="ctl">状态码 <input v-model="auditFilter.status_code" placeholder="如 429" style="width: 70px"></label>
        <label class="ctl">近
          <select v-model="auditFilter.hours">
            <option :value="1">1h</option>
            <option :value="6">6h</option>
            <option :value="24">24h</option>
            <option :value="72">3d</option>
            <option :value="168">7d</option>
          </select>
        </label>
        <button class="btn primary" :disabled="auditLoading" @click="searchAudit">
          {{ auditLoading ? '查询中…' : '查询' }}
        </button>
      </div>
      <p v-if="auditErr" class="chat-err">{{ auditErr }}</p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>时间</th><th>客户</th><th>模型</th><th>Provider</th><th>任务/Agent</th>
              <th class="num">输入</th><th class="num">输出</th><th>状态码</th><th>事件ID</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="!auditEvents.length && !auditLoading">
              <td colspan="9" class="empty">暂无事件（调整筛选或时间范围）</td>
            </tr>
            <tr v-for="e in auditEvents" :key="e.event_id">
              <td class="small">{{ fmtTime(e.event_time) }}</td>
              <td class="small">{{ customerName(e.customer_id) }}</td>
              <td>{{ e.model }}</td>
              <td>{{ e.provider }}</td>
              <td class="small">{{ e.task_type }}{{ e.agent_id ? ' / ' + e.agent_id : '' }}</td>
              <td class="num">{{ fmtNum(e.input_tokens) }}</td>
              <td class="num">{{ fmtNum(e.output_tokens) }}</td>
              <td>
                <span class="badge" :class="statusBadge(e.status_code)">{{ e.status_code || '-' }}</span>
              </td>
              <td class="small muted" :title="e.transaction_id">{{ (e.event_id || '').slice(0, 8) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="pager" v-if="auditEvents.length">
        <button class="btn" :disabled="auditFilter.offset === 0 || auditLoading" @click="turnPage(-auditFilter.limit)">上一页</button>
        <span class="muted small">第 {{ auditFilter.offset / auditFilter.limit + 1 }} 页（每页 {{ auditFilter.limit }} 条）</span>
        <button class="btn" :disabled="auditEvents.length < auditFilter.limit || auditLoading" @click="turnPage(auditFilter.limit)">下一页</button>
      </div>
    </div>

    <!-- 调试沙盒 -->
    <div v-else-if="tab === 'debug'" class="grid-2">
      <div class="card">
        <div class="card-head"><h2>在线调试沙盒</h2></div>
        <p class="muted small" style="margin-bottom: 12px">
          走网关完整链路（鉴权→白名单→限流→预算→路由→定价→计量），用当前登录密钥的真实权限执行；调用记录进入审计日志可回看。
        </p>
        <div class="form-col">
          <label class="ctl">模型
            <input v-model="debugForm.model" placeholder="模型名" list="debug-models">
            <datalist id="debug-models">
              <option v-for="m in modelsCache" :key="m" :value="m"></option>
            </datalist>
          </label>
          <label class="ctl">客户（可选；携带 X-Meteron-Customer-ID）
            <select v-model="debugForm.customer_id">
              <option value="">（不指定）</option>
              <option v-for="c in customers" :key="c.id" :value="c.id">{{ c.name }}</option>
            </select>
          </label>
          <div class="form-row">
            <label class="ctl">任务类型 <input v-model="debugForm.task_type" placeholder="chat"></label>
            <label class="ctl">Agent ID <input v-model="debugForm.agent_id" placeholder="debug-sandbox"></label>
            <label class="ctl">max_tokens <input v-model="debugForm.max_tokens" type="number" style="width: 90px"></label>
          </div>
          <label class="ctl" style="align-items: flex-start">消息
            <textarea v-model="debugForm.message" rows="4" placeholder="发送给模型的消息"
              style="flex:1; resize: vertical; min-height: 80px"></textarea>
          </label>
          <button class="btn primary" :disabled="debugging" @click="runDebug">
            {{ debugging ? '调用中…' : '发起调试调用' }}
          </button>
          <p v-if="debugErr" class="chat-err">{{ debugErr }}</p>
          <p v-if="debugStatus" class="muted small">
            HTTP {{ debugStatus }} · 耗时 {{ debugMs }}ms
          </p>
        </div>
      </div>

      <div class="card">
        <div class="card-head"><h2>响应</h2>
          <button v-if="debugResult" class="btn" @click="copyResult">复制</button>
        </div>
        <pre class="debug-result">{{ debugResult || '（尚未调用）' }}</pre>
      </div>
    </div>
  </div>
</template>

<style scoped>
.admin-tabs { margin-bottom: 4px; }
.small-btn { padding: 3px 10px; font-size: 12px; margin-right: 6px; }
.danger-btn { color: var(--red); }
.danger-btn:hover { border-color: var(--red); }
.form-col { display: flex; flex-direction: column; gap: 12px; }
.form-col .ctl { flex-direction: column; align-items: stretch; gap: 4px; display: flex; }
.form-row { display: flex; gap: 10px; }
.form-row .ctl { flex: 1; flex-direction: column; align-items: stretch; gap: 4px; display: flex; }
.created-key { margin-top: 6px; padding: 10px; border: 1px dashed var(--accent); border-radius: 8px; }
.key-plaintext { word-break: break-all; color: var(--accent); font-size: 12px; }
.audit-filter { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; margin-bottom: 14px; }
.audit-filter .ctl { gap: 6px; }
.err-text { color: var(--red); }
.pager { display: flex; align-items: center; gap: 12px; margin-top: 12px; }
.debug-result {
  background: rgba(11, 14, 20, .6); border: 1px solid var(--border); border-radius: 10px;
  padding: 12px; font-size: 12px; line-height: 1.5; white-space: pre-wrap; word-break: break-all;
  max-height: 520px; overflow-y: auto; min-height: 120px;
}
</style>
