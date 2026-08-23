<script setup>
import { ref, reactive, nextTick, onMounted, computed } from 'vue'
import { session, api } from '../api.js'

// 大语言模型聊天窗口：走网关 /v1/chat/completions（SSE 流式），
// 每轮对话经过完整的计量链路（预算预留 → 转发 → usage 确认 → Kafka 计量）。
const props = defineProps({ customers: { type: Array, default: () => [] } })

const models = ref([])
const model = ref('')
const customer = ref('')
const input = ref('')
const sending = ref(false)
const errMsg = ref('')
const listEl = ref(null)

const messages = reactive([]) // { role:'user'|'assistant', content, usage? }

const customerOptions = computed(() =>
  props.customers.map((c) => ({ value: c.external_id || c.id, label: c.name })))

onMounted(async () => {
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

function modelLabel(m) {
  const info = models.value.find((x) => x.model === m)
  return info ? `${m}（${info.provider}）` : m
}

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
  scrollBottom()

  // 携带历史上下文（与展示一致，最多截取最近 20 条防 body 膨胀）
  const history = messages.slice(0, -2).slice(-20)
    .map((m) => ({ role: m.role, content: m.content }))

  sending.value = true
  try {
    const res = await fetch('/v1/chat/completions', {
      method: 'POST',
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

    // 解析 SSE 流：delta.content 增量拼接，末 chunk 带 usage
    const reader = res.body.getReader()
    const dec = new TextDecoder()
    let buf = ''
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
        if (payload === '[DONE]') continue
        try {
          const j = JSON.parse(payload)
          const delta = j.choices?.[0]?.delta
          // 思考模型（qwen3 等）先输出 reasoning 再输出 content，两者都要渲染
          if (delta?.reasoning) { reply.reasoning = (reply.reasoning || '') + delta.reasoning; scrollBottom() }
          if (delta?.content) { reply.content += delta.content; scrollBottom() }
          if (j.usage) reply.usage = j.usage
        } catch { /* 忽略不完整/非 JSON 行 */ }
      }
    }
    if (!reply.content && !reply.reasoning) reply.content = '（空响应）'
  } catch (e) {
    reply.content = '⚠ ' + e.message
  } finally {
    sending.value = false
    scrollBottom()
  }
}

function clearChat() {
  messages.splice(0, messages.length)
  errMsg.value = ''
}
</script>

<template>
  <section class="card">
    <div class="card-head">
      <h2>模型对话</h2>
      <span class="muted small">经网关计量：预算预留 → 转发上游 → usage 确认入账</span>
      <span class="spacer"></span>
      <button class="btn ghost" @click="clearChat" :disabled="!messages.length || sending">清空</button>
    </div>

    <div class="chat-toolbar">
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
    </div>

    <div class="chat-list" ref="listEl">
      <div v-if="!messages.length" class="chat-empty">
        与模型对话，token 用量将实时进入下方计量看板（默认模型：{{ models.length ? models[0].model : '-' }}）
      </div>
      <div v-for="(m, i) in messages" :key="i" class="chat-msg" :class="m.role">
        <div class="bubble">
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
        <div class="bubble"><span class="typing">等待响应…</span></div>
      </div>
    </div>

    <p class="chat-err">{{ errMsg }}</p>

    <div class="chat-input">
      <textarea v-model="input" rows="2" placeholder="输入消息，Enter 发送，Shift+Enter 换行"
        :disabled="sending" @keydown.enter.exact.prevent="send"></textarea>
      <button class="btn primary" @click="send" :disabled="sending || !input.trim()">发送</button>
    </div>
  </section>
</template>
