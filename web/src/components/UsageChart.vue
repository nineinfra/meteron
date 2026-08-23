<script setup>
import { computed } from 'vue'

const props = defineProps({ points: { type: Array, default: () => [] } })

// 超过 96 点按天聚合（30d 视图）
const data = computed(() => {
  if (props.points.length <= 96) {
    return props.points.map((p) => ({
      hour: p.hour, in: p.input_tokens, out: p.output_tokens,
    }))
  }
  const byDay = new Map()
  for (const p of props.points) {
    const d = p.hour.slice(0, 10)
    const cur = byDay.get(d) || { in: 0, out: 0 }
    cur.in += p.input_tokens
    cur.out += p.output_tokens
    byDay.set(d, cur)
  }
  return [...byDay.entries()].sort().map(([d, v]) => ({ hour: d, in: v.in, out: v.out }))
})

const dayBucket = computed(() => props.points.length > 96)

function fmt(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return String(n)
}

const chart = computed(() => {
  const d = data.value
  if (!d.length) return null
  const W = 1080, H = 220, padL = 52, padB = 26, padT = 12
  const plotW = W - padL - 12, plotH = H - padT - padB
  const max = Math.max(1, ...d.map((x) => x.in + x.out))
  const bw = plotW / d.length
  const barW = Math.max(1.5, Math.min(28, bw * 0.62))

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map((f) => Math.round(max * f))
  const grid = yTicks.map((v) => {
    const y = padT + plotH - (v / max) * plotH
    return { y, label: fmt(v) }
  })

  const bars = d.map((x, i) => {
    const bx = padL + i * bw + (bw - barW) / 2
    const hIn = (x.in / max) * plotH
    const hOut = (x.out / max) * plotH
    const yIn = padT + plotH - hIn
    return {
      x: bx, yIn, hIn, yOut: yIn - hOut, hOut,
      title: (dayBucket.value ? x.hour : x.hour.slice(5, 10) + ' ' + x.hour.slice(11, 13) + '时')
        + `｜输入 ${x.in.toLocaleString()}｜输出 ${x.out.toLocaleString()}`,
    }
  })

  const step = Math.ceil(d.length / 8)
  const xTicks = d
    .map((x, i) => ({
      i,
      x: padL + i * bw + bw / 2,
      // 小时粒度带 "时" 区分同日各点；天粒度仅日期
      label: dayBucket.value ? x.hour.slice(5) : x.hour.slice(5, 10) + ' ' + x.hour.slice(11, 13) + '时',
    }))
    .filter((t) => t.i % step === 0)

  return { W, H, padL, padT, plotH, grid, bars, xTicks, axisY: padT + plotH, barW }
})
</script>

<template>
  <div class="chart">
    <p v-if="!chart" class="muted" style="padding:40px 0;text-align:center">时间范围内暂无计量数据</p>
    <svg v-else :viewBox="`0 0 ${chart.W} ${chart.H}`" preserveAspectRatio="none">
      <!-- 网格与 y 轴刻度 -->
      <template v-for="(g, i) in chart.grid" :key="i">
        <line class="gridline" :x1="chart.padL" :x2="chart.W - 12" :y1="g.y" :y2="g.y" />
        <text class="tick" :x="chart.padL - 6" :y="g.y + 3" text-anchor="end">{{ g.label }}</text>
      </template>
      <!-- x 轴 -->
      <line class="axis" :x1="chart.padL" :x2="chart.W - 12" :y1="chart.axisY" :y2="chart.axisY" />
      <!-- 柱状：输出堆叠在输入上方 -->
      <g v-for="(b, i) in chart.bars" :key="i">
        <title>{{ b.title }}</title>
        <rect class="bar-in" :x="b.x" :y="b.yIn" :width="chart.barW" :height="b.hIn" rx="1.5" />
        <rect class="bar-out" :x="b.x" :y="b.yOut" :width="chart.barW" :height="b.hOut" rx="1.5" />
      </g>
      <!-- x 轴标签 -->
      <text v-for="(t, i) in chart.xTicks" :key="'x' + i" class="tick" :x="t.x" :y="chart.H - 8" text-anchor="middle">{{ t.label }}</text>
    </svg>
  </div>
</template>
