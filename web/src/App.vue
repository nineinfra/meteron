<script setup>
import { ref, reactive, computed, onMounted, onUnmounted } from 'vue'
import { session, clearSession, api, UnauthorizedError } from './api.js'
import MonitorView from './components/MonitorView.vue'
import ChatView from './components/ChatView.vue'
import AdminView from './components/AdminView.vue'

const connected = ref(false)
const connStatus = ref('')
const connErr = ref('')
const gateKey = ref('')

const state = reactive({ customers: [] })

// ---- hash 路由：#/monitor（默认）| #/chat | #/admin ----
const menus = [
  { key: 'monitor', label: '监控看板', icon: '◧' },
  { key: 'chat', label: '模型对话', icon: '✳' },
  { key: 'admin', label: '平台管理', icon: '⚙' },
]
const route = ref('monitor')

function applyHash() {
  const h = location.hash.replace(/^#\/?/, '')
  route.value = menus.some((m) => m.key === h) ? h : 'monitor'
}

function go(key) {
  location.hash = '#/' + key
}

async function connect(key) {
  connErr.value = ''
  if (!key) { connErr.value = '请输入 API Key'; return }
  session.key = key
  try {
    await api('/api/v1/customers') // 探活 + 鉴权校验
    connected.value = true
    connStatus.value = '已连接'
    await loadCustomers()
  } catch (e) {
    session.key = ''
    if (!(e instanceof UnauthorizedError)) connErr.value = '连接失败：' + e.message
  }
}

function disconnect() {
  clearSession()
  connected.value = false
  state.customers = []
}

async function loadCustomers() {
  try {
    state.customers = await api('/api/v1/customers')
  } catch (e) {
    if (e instanceof UnauthorizedError) disconnect()
  }
}

function onExpired() { // 子视图 401：会话失效
  disconnect()
}

const pageTitle = computed(() => menus.find((m) => m.key === route.value)?.label || '监控看板')

onMounted(() => {
  applyHash()
  window.addEventListener('hashchange', applyHash)
  if (session.key) connect(session.key)
})
onUnmounted(() => window.removeEventListener('hashchange', applyHash))
</script>

<template>
  <!-- 未连接：引导页 -->
  <div v-if="!connected" class="gate">
    <div class="gate-box">
      <div class="logo big">M</div>
      <h1>Meteron 控制台</h1>
      <p class="gate-tip">输入 API Key 连接本实例</p>
      <input v-model="gateKey" type="password" placeholder="API Key（Bearer Token）"
        autocomplete="off" @keydown.enter="connect(gateKey.trim())">
      <button class="btn primary wide" @click="connect(gateKey.trim())">连接</button>
      <p class="gate-err">{{ connErr }}</p>
    </div>
  </div>

  <!-- 已连接：左侧菜单布局 -->
  <div v-else class="layout">
    <aside class="sidebar">
      <div class="brand">
        <span class="logo">M</span>
        <span class="name">Meteron</span>
      </div>
      <nav class="menu">
        <a v-for="m in menus" :key="m.key" :class="{ active: route === m.key }"
          :href="'#/' + m.key" @click="go(m.key)">
          <span class="icon">{{ m.icon }}</span>{{ m.label }}
        </a>
      </nav>
      <div class="sidebar-foot">
        <div class="status" :class="{ ok: connStatus === '已连接' }">{{ connStatus }}</div>
        <button class="btn ghost wide" @click="disconnect">断开连接</button>
      </div>
    </aside>

    <div class="main">
      <header class="topbar">
        <span class="page-title">{{ pageTitle }}</span>
        <span class="muted small">LLM 计量计费控制台</span>
      </header>
      <main class="content">
        <MonitorView v-show="route === 'monitor'" :customers="state.customers" @expired="onExpired" />
        <ChatView v-show="route === 'chat'" :customers="state.customers" />
        <AdminView v-show="route === 'admin'" :customers="state.customers" @expired="onExpired" />
      </main>
    </div>
  </div>
</template>

<style src="./style.css"></style>
