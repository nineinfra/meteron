<script setup>
import { ref, reactive, computed, onMounted, onUnmounted } from 'vue'
import { session, persistSession, api, UnauthorizedError, fmtAmount, fmtPeriod, fmtTime } from '../api.js'
import UsageChart from './UsageChart.vue'

const props = defineProps({ customers: { type: Array, default: () => [] }, connected: Boolean })
const emit = defineEmits(['expired'])

const state = reactive({
  usage: [],
  points: [],
  pools: [],
  bills: [],
})
const updated = ref('')
const status = ref('')
let timer = null

const hoursOptions = [24, 168, 720]

const kpis = computed(() => {
  const t = state.usage.reduce((a, i) => ({
    in: a.in + i.input_tokens,
    out: a.out + i.output_tokens,
    cache: a.cache + i.cached_input_tokens,
    req: a.req + i.request_count,
  }), { in: 0, out: 0, cache: 0, req: 0 })
  return [
    { label: `请求数（${session.hours}h）`, value: fmt(t.req), cls: '' },
    { label: `输入 Tokens（${session.hours}h）`, value: fmt(t.in), cls: 'in' },
    { label: `输出 Tokens（${session.hours}h）`, value: fmt(t.out), cls: 'out' },
    { label: `缓存命中 Tokens（${session.hours}h）`, value: fmt(t.cache), cls: 'cache' },
  ]
})

function fmt(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return String(n)
}

function customerName(id) {
  const c = props.customers.find((x) => x.id === id)
  return c ? c.name : (id ? id.slice(0, 8) + '…' : '-')
}

function setHours(h) {
  session.hours = h
  persistSession()
  refresh()
}

function setCustomer(v) {
  session.customer = v
  persistSession()
  refresh()
}

function range() {
  const to = new Date()
  const from = new Date(to.getTime() - session.hours * 3600e3)
  return { start_time: from.toISOString(), end_time: to.toISOString() }
}

function qs(extra = {}) {
  const p = new URLSearchParams()
  if (session.customer) p.set('customer_id', session.customer)
  for (const [k, v] of Object.entries(extra)) p.set(k, v)
  return p.toString()
}

async function refresh() {
  status.value = '刷新中…'
  const r = range()
  try {
    const [usage, series, pools, bills] = await Promise.all([
      api('/api/v1/usage?' + qs(r)),
      api('/api/v1/usage/timeseries?' + qs(r)),
      api('/api/v1/budget-pools?' + qs()),
      api('/api/v1/bills?' + qs()),
    ])
    state.usage = usage.items || []
    state.points = series.points || []
    state.pools = pools || []
    state.bills = bills || []
    updated.value = '更新于 ' + new Date().toLocaleTimeString('zh-CN', { hour12: false })
    status.value = '已连接'
  } catch (e) {
    if (e instanceof UnauthorizedError) emit('expired')
    else status.value = '加载失败：' + e.message
  }
}

async function finalizeBill(id) {
  if (!confirm('定稿后账单不可变更，确认继续？')) return
  try {
    await api(`/api/v1/bills/${id}/finalize`, { method: 'POST', body: '{}' })
    await refresh()
  } catch (e) {
    alert('定稿失败：' + e.message)
  }
}

onMounted(() => {
  refresh()
  timer = setInterval(refresh, 60_000) // 自动刷新
})
onUnmounted(() => { if (timer) clearInterval(timer) })
defineExpose({ refresh })
</script>

<template>
  <div class="view">
    <!-- 筛选栏 -->
    <section class="controls card">
      <label class="ctl">客户
        <select :value="session.customer" @change="setCustomer($event.target.value)">
          <option value="">全部客户</option>
          <option v-for="c in customers" :key="c.id" :value="c.id">
            {{ c.name }}{{ c.external_id ? `（${c.external_id}）` : '' }}
          </option>
        </select>
      </label>
      <div class="ctl range">
        <button v-for="h in hoursOptions" :key="h" :class="{ active: session.hours === h }" @click="setHours(h)">
          {{ h === 24 ? '24小时' : h === 168 ? '7天' : '30天' }}
        </button>
      </div>
      <span class="spacer"></span>
      <span class="muted small">{{ status }}</span>
      <span class="updated muted">{{ updated }}</span>
    </section>

    <!-- KPI -->
    <section class="kpis">
      <div v-for="k in kpis" :key="k.label" class="kpi">
        <div class="label">{{ k.label }}</div>
        <div class="value" :class="k.cls">{{ k.value }}</div>
      </div>
    </section>

    <!-- 趋势图 -->
    <section class="card">
      <div class="card-head">
        <h2>Token 用量趋势</h2>
        <div class="legend">
          <span><i class="dot in"></i>输入</span>
          <span><i class="dot out"></i>输出</span>
        </div>
      </div>
      <UsageChart :points="state.points" />
    </section>

    <div class="grid-2">
      <!-- 用量明细 -->
      <section class="card">
        <div class="card-head"><h2>用量明细</h2><span class="muted small">按模型聚合</span></div>
        <div class="table-wrap">
          <table>
            <thead><tr>
              <th>模型</th><th>Provider</th><th>任务</th>
              <th class="num">输入</th><th class="num">输出</th><th class="num">缓存</th><th class="num">请求</th>
            </tr></thead>
            <tbody>
              <tr v-if="!state.usage.length"><td colspan="7" class="empty">暂无用量数据</td></tr>
              <tr v-for="(i, idx) in [...state.usage].sort((a, b) => (b.input_tokens + b.output_tokens) - (a.input_tokens + a.output_tokens))" :key="idx">
                <td>{{ i.model || '-' }}</td>
                <td class="muted">{{ i.provider || '-' }}</td>
                <td><span class="badge draft">{{ i.task_type || '-' }}</span></td>
                <td class="num">{{ fmt(i.input_tokens) }}</td>
                <td class="num">{{ fmt(i.output_tokens) }}</td>
                <td class="num">{{ fmt(i.cached_input_tokens) }}</td>
                <td class="num">{{ fmt(i.request_count) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <!-- 预算池 -->
      <section class="card">
        <div class="card-head"><h2>预算池</h2><span class="muted small">实时余额（PG 可信源）</span></div>
        <div v-if="!state.pools.length" class="muted" style="padding:24px 0;text-align:center">暂无预算池</div>
        <div v-else class="pools">
          <div v-for="p in state.pools" :key="p.id" class="pool">
            <div class="pool-head">
              <div>
                <span class="pool-name">{{ customerName(p.customer_id) }}</span>
                <span class="muted small">{{ p.task_type ? ' / ' + p.task_type : '' }}{{ p.agent_id ? ' / ' + p.agent_id : '' }}</span>
              </div>
              <span class="badge" :class="p.exhaust_policy">
                {{ { block: '超支阻断', warn: '超支告警', fallback: '自动降级' }[p.exhaust_policy] || p.exhaust_policy }}
              </span>
            </div>
            <div class="pool-bar">
              <div class="pool-bar-fill"
                :class="{ warn: (Number(p.spent_amount) / Number(p.max_budget)) * 100 >= 80 && (Number(p.spent_amount) / Number(p.max_budget)) * 100 < 100, danger: (Number(p.spent_amount) / Number(p.max_budget)) * 100 >= 100 }"
                :style="{ width: Math.min(100, (Number(p.spent_amount) / Number(p.max_budget)) * 100 || 0) + '%' }"></div>
            </div>
            <div class="pool-nums">
              <span>已花费 <b>{{ Number(p.spent_amount || 0).toFixed(4) }}</b> / {{ p.max_budget }}</span>
              <span>在途预留 {{ Number(p.pending_reserved || 0).toFixed(4) }}</span>
              <span>可用 <b>{{ p.available_amount }}</b></span>
            </div>
          </div>
        </div>
      </section>
    </div>

    <!-- 账单 -->
    <section class="card">
      <div class="card-head"><h2>账单</h2><span class="muted small">draft 可定稿，finalized 不可变更</span></div>
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>客户</th><th>账期</th><th class="num">金额</th><th>状态</th><th>定稿时间</th><th></th>
          </tr></thead>
          <tbody>
            <tr v-if="!state.bills.length"><td colspan="6" class="empty">暂无账单</td></tr>
            <tr v-for="b in state.bills" :key="b.id">
              <td>{{ customerName(b.customer_id) }}</td>
              <td class="muted">{{ fmtPeriod(b.period_start, b.period_end) }}</td>
              <td class="num"><b>{{ fmtAmount(b.total_amount, b.currency) }}</b></td>
              <td><span class="badge" :class="b.status">{{ b.status === 'finalized' ? '已定稿' : '草稿' }}</span></td>
              <td class="muted small">{{ fmtTime(b.finalized_at) }}</td>
              <td>
                <button v-if="b.status === 'draft'" class="btn" @click="finalizeBill(b.id)">定稿</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </div>
</template>
