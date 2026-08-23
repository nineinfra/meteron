<script setup>
import { ref, reactive, nextTick, onMounted, computed } from 'vue'
import { session, api } from '../api.js'

// 模型对话页：走网关 /v1/chat/completions（SSE 流式），
// 每轮对话经过完整的计量链路（预算预留 → 转发 → usage 确认 → Kafka 计量）。
const props = defineProps({ customers: { type: Array, default: () => [] } })

const models = ref([])
const model = ref('')
const customer = ref('')
const input = ref('')
const sending = ref(false)
const errMsg = ref('')
const listEl = ref(null)

// 等待计时（本地大模型首 token 可达数十秒，给用户"未卡死"的反馈）
const waitingSec = ref(0)
let waitTimer = null
let abortCtl = null // 停止按钮：中断当前请求（联动取消上游推理）

function startWait() {
  waitingSec.value = 0
  waitTimer = setInterval(() => { waitingSec.value++ }, 1000)
}
function stopWait() {
  if (waitTimer) { clearInterval(waitTimer); waitTimer = null }
  waitingSec.value = 0
}

// 停止当前生成：fetch abort → 网关感知断开 → 取消上游请求（释放 Ollama 排队槽）
function stop() {
  if (abortCtl) abortCtl.abort()
}

// 复制消息内容（clipboard API 失败时回退 execCommand，兼容非安全上下文/LAN 访问）
async function copyMsg(m) {
  const text = m.content || ''
  if (!text) return
  let ok = false
  try { await navigator.clipboard.writeText(text); ok = true } catch {
    try {
      const ta = document.createElement('textarea')
      ta.value = text
      ta.style.cssText = 'position:fixed;opacity:0'
      document.body.appendChild(ta)
      ta.select()
      ok = document.execCommand('copy')
      ta.remove()
    } catch { ok = false }
  }
  if (ok) {
    m._copied = true
    setTimeout(() => { m._copied = false }, 1500)
  }
}

const messages = reactive([]) // { role:'user'|'assistant', content, reasoning?, usage? }

// 聊天历史持久化：localStorage 保存最近 200 条，刷新/重开浏览器不丢
const HISTORY_KEY = 'meteron_chat_history_v1'
const HISTORY_CAP = 200

function saveHistory() {
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(messages.slice(-HISTORY_CAP)))
  } catch { /* 存储满等异常：静默放弃 */ }
}

function loadHistory() {
  try {
    const saved = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]')
    if (Array.isArray(saved) && saved.length) messages.push(...saved)
  } catch { /* 损坏数据：当作无历史 */ }
}

const customerOptions = computed(() =>
  props.customers.map((c) => ({ value: c.external_id || c.id, label: c.name })))

// 会话累计（本页内存）：轮次与 token 汇总
const sessionStat = computed(() => {
  let turns = 0, inTok = 0, outTok = 0
  for (const m of messages) {
    if (m.role === 'assistant') {
      turns++
      inTok += m.usage?.prompt_tokens || 0
      outTok += m.usage?.completion_tokens || 0
    }
  }
  return { turns, inTok, outTok }
})

onMounted(async () => {
  loadHistory()
  try {
    models.value = await api('/api/v1/models')
    if (models.value.length && !model.value) model.value = models.value[0].model
  } catch { /* 网络失败时仍可手输 */ }
  if (!customer.value) {
    const saved = session.customer
    const hit = customerOptions.value.find((c) => c.value === saved)
    customer.value = hit ? hit.value : (customerOptions.value[0]?.value || '')
  }
})

function scrollBottom() {
  nextTick(() => {
    if (listEl.value) listEl.value.scrollTop = listEl.value.scrollHeight
  })
}

async function send() {
  const text = input.value.trim()
  if (!text || sending.value) return
  if (!customer.value) { errMsg.value = '请先选择客户（对话需挂靠客户计量计费）'; return }
  if (!model.value) { errMsg.value = '请先选择或输入模型名'; return }

  errMsg.value = ''
  input.value = ''
  messages.push({ role: 'user', content: text })
  const reply = reactive({ role: 'assistant', content: '', reasoning: '', usage: null })
  messages.push(reply)
  saveHistory()
  scrollBottom()

  // 携带历史上下文（与展示一致，最多截取最近 20 条防 body 膨胀）
  const history = messages.slice(0, -2).slice(-20)
    .map((m) => ({ role: m.role, content: m.content }))

  sending.value = true
  startWait()
  abortCtl = new AbortController()
  try {
    const res = await fetch('/v1/chat/completions', {
      method: 'POST',
      signal: abortCtl.signal,
      headers: {
        Authorization: 'Bearer ' + session.key,
        'Content-Type': 'application/json',
        'X-Meteron-Customer-ID': customer.value,
        'X-Meteron-Task-Type': 'chat',
      },
      body: JSON.stringify({
        model: model.value,
        messages: [...history, { role: 'user', content: text }],
        stream: true,
      }),
    })
    if (!res.ok) { // 网关错误信封 {code,msg,...}
      let msg = 'HTTP ' + res.status
      try { const e = await res.json(); if (e.msg) msg = e.msg } catch { /* 非 JSON */ }
      throw new Error(msg)
    }

    // 解析 SSE 流：delta.content / delta.reasoning 增量拼接，末 chunk 带 usage
    const reader = res.body.getReader()
    const dec = new TextDecoder()
    let buf = ''
    let sawDone = false // 正常结束必有 data: [DONE]；缺失说明流被中途截断
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      buf += dec.decode(value, { stream: true })
      let nl
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).trim()
        buf = buf.slice(nl + 1)
        if (!line.startsWith('data:')) continue
        const payload = line.slice(5).trim()
        if (payload === '[DONE]') { sawDone = true; continue }
        try {
          const j = JSON.parse(payload)
          const delta = j.choices?.[0]?.delta
          // 思考模型（qwen3 等）先输出 reasoning 再输出 content，两者都要渲染
          if (delta?.reasoning) { reply.reasoning = (reply.reasoning || '') + delta.reasoning; stopWait(); scrollBottom() }
          if (delta?.content) { reply.content += delta.content; stopWait(); scrollBottom() }
          if (j.usage) reply.usage = j.usage
        } catch { /* 忽略不完整/非 JSON 行 */ }
      }
    }
    if (!sawDone && !reply.usage) {
      // 流中断：正文/usage 未送达（如上游超时、连接断开）
      reply.content = (reply.content || '') + '\n\n⚠ 连接中断，回复可能不完整'
    } else if (!reply.content && !reply.reasoning) {
      reply.content = '（空响应）'
    }
  } catch (e) {
    if (e.name === 'AbortError') {
      reply.content = (reply.content ? reply.content + '\n\n' : '') + '⏹ 已停止'
    } else {
      reply.content = '⚠ ' + e.message
    }
  } finally {
    stopWait()
    sending.value = false
    abortCtl = null
    saveHistory()
    scrollBottom()
  }
}

function clearChat() {
  messages.splice(0, messages.length)
  saveHistory()
  errMsg.value = ''
}
</script>

<template>
  <div class="view chat-view">
    <!-- 工具栏 -->
    <section class="card chat-bar">
      <label class="ctl">模型
        <input v-model="model" list="model-options" placeholder="模型名" :disabled="sending">
        <datalist id="model-options">
          <option v-for="m in models" :key="m.model" :value="m.model">{{ m.provider }} / {{ m.type }}</option>
        </datalist>
      </label>
      <label class="ctl">客户
        <select v-model="customer" :disabled="sending">
          <option v-if="!customerOptions.length" value="">（请先创建客户）</option>
          <option v-for="c in customerOptions" :key="c.value" :value="c.value">{{ c.label }}</option>
        </select>
      </label>
      <span class="spacer"></span>
      <span class="muted small" v-if="sessionStat.turns">
        本会话：{{ sessionStat.turns }} 轮 · ↑{{ sessionStat.inTok }} ↓{{ sessionStat.outTok }} tokens
      </span>
      <button class="btn ghost" @click="clearChat" :disabled="!messages.length || sending">清空会话</button>
    </section>

    <!-- 消息列表（占满剩余高度） -->
    <div class="chat-list grow" ref="listEl">
      <div v-if="!messages.length" class="chat-empty">
        <div class="chat-welcome">
          <p>与模型对话，token 用量将实时进入「监控看板」</p>
          <p class="muted small">默认模型：{{ models.length ? models[0].model : '-' }}（可在上方切换）</p>
        </div>
      </div>
      <div v-for="(m, i) in messages" :key="i" class="chat-msg" :class="m.role">
        <div class="bubble">
          <button v-if="m.content" class="copy-btn" :class="{ ok: m._copied }" @click="copyMsg(m)">
            {{ m._copied ? '已复制' : '复制' }}
          </button>
          <details v-if="m.reasoning" class="chat-reasoning" :open="sending && i === messages.length - 1 && !m.content">
            <summary>思考过程</summary>
            <pre class="chat-text reasoning-text">{{ m.reasoning }}</pre>
          </details>
          <pre class="chat-text">{{ m.content }}</pre>
          <div v-if="m.role === 'assistant' && m.usage" class="chat-usage">
            <span>↑{{ m.usage.prompt_tokens }}</span>
            <span>↓{{ m.usage.completion_tokens }}</span>
            <span v-if="m.usage.prompt_tokens_details?.cached_tokens">缓存 {{ m.usage.prompt_tokens_details.cached_tokens }}</span>
          </div>
        </div>
      </div>
      <div v-if="sending && !(messages[messages.length - 1]?.content || messages[messages.length - 1]?.reasoning)" class="chat-msg assistant">
        <div class="bubble">
          <span class="typing">等待响应… {{ waitingSec }}s</span>
          <button class="stop-btn" @click="stop">停止</button>
        </div>
      </div>
    </div>

    <p class="chat-err">{{ errMsg }}</p>

    <!-- 输入区 -->
    <div class="chat-input">
      <textarea v-model="input" rows="2" placeholder="输入消息，Enter 发送，Shift+Enter 换行"
        :disabled="sending" @keydown.enter.exact.prevent="send"></textarea>
      <button v-if="sending" class="btn stop-wide" @click="stop">停止</button>
      <button class="btn primary" @click="send" :disabled="sending || !input.trim()">发送</button>
    </div>
  </div>
</template>
