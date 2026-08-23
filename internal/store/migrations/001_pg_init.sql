-- Meteron PG schema (详细设计文档 V1.0 第4章)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 通用枚举定义
CREATE TYPE commitment_type AS ENUM ('prepaid', 'postpaid');
CREATE TYPE rollover_policy AS ENUM ('expire', 'rollover');
CREATE TYPE exhaust_policy AS ENUM ('warn', 'block', 'fallback');
CREATE TYPE reservation_status AS ENUM ('pending', 'confirmed', 'released', 'expired');
CREATE TYPE bill_status AS ENUM ('draft', 'amending', 'finalized', 'voided');
CREATE TYPE aggregation_type AS ENUM ('sum', 'count', 'max', 'unique_count');
CREATE TYPE pricing_type AS ENUM ('flat', 'tiered');
CREATE TYPE billing_run_status AS ENUM ('pending','running','completed','failed');
CREATE TYPE reconciliation_run_status AS ENUM ('pending','running','completed','failed');
CREATE TYPE webhook_delivery_status AS ENUM ('pending','delivering','success','failed','dead_letter');

-- 租户表
CREATE TABLE tenants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP
);
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;

-- 客户表
CREATE TABLE customers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    name VARCHAR(255) NOT NULL,
    external_id VARCHAR(255),
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    UNIQUE(tenant_id, external_id)
);
CREATE INDEX idx_customer_tenant ON customers(tenant_id);
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;

-- api_keys 表：密钥 AES 加密存储，不保存明文
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID REFERENCES customers(id),
    role_id UUID,
    key_hash TEXT NOT NULL,
    scope JSONB NOT NULL,
    expire_at TIMESTAMP,
    is_enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP
);
CREATE INDEX idx_apikey_tenant ON api_keys(tenant_id, is_enabled);
ALTER TABLE api_keys ENABLE ROW LEVEL SECURITY;

-- Billable Metric
CREATE TABLE billable_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    aggregation_type aggregation_type NOT NULL,
    group_keys TEXT[] NOT NULL,
    is_locked BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    UNIQUE(tenant_id, name)
);
CREATE INDEX idx_billable_metric_tenant ON billable_metrics(tenant_id);
ALTER TABLE billable_metrics ENABLE ROW LEVEL SECURITY;

-- Product
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    metric_id UUID NOT NULL REFERENCES billable_metrics(id),
    name VARCHAR(255) NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    UNIQUE(tenant_id, name)
);
CREATE INDEX idx_product_tenant_metric ON products(tenant_id, metric_id);
ALTER TABLE products ENABLE ROW LEVEL SECURITY;

-- RateCard 费率卡
CREATE TABLE rate_cards (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    name VARCHAR(255) NOT NULL,
    version INT NOT NULL DEFAULT 1,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    is_active BOOLEAN NOT NULL DEFAULT true,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_default BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    UNIQUE(tenant_id, name, version)
);
CREATE INDEX idx_ratecard_tenant_active ON rate_cards(tenant_id, is_active, valid_from);
ALTER TABLE rate_cards ENABLE ROW LEVEL SECURITY;

-- RateCard 匹配规则 + 定价配置（flat / tiered）
CREATE TABLE rate_card_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rate_card_id UUID NOT NULL REFERENCES rate_cards(id),
    product_id UUID NOT NULL REFERENCES products(id),
    match_conditions JSONB NOT NULL,
    pricing_type pricing_type NOT NULL,
    pricing_config JSONB NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    is_default_rule BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP
);
CREATE INDEX idx_ratecard_rule_rc ON rate_card_rules(rate_card_id);
ALTER TABLE rate_card_rules ENABLE ROW LEVEL SECURITY;

-- Contract 客户合同
CREATE TABLE contracts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL REFERENCES customers(id),
    rate_card_id UUID NOT NULL REFERENCES rate_cards(id),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    is_terminated BOOLEAN NOT NULL DEFAULT false,
    custom_rate_overrides JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP
);
CREATE INDEX idx_contract_tenant_customer ON contracts(tenant_id, customer_id, start_time, end_time);
ALTER TABLE contracts ENABLE ROW LEVEL SECURITY;

-- Commit 保底额度
CREATE TABLE commitments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    contract_id UUID NOT NULL REFERENCES contracts(id),
    type commitment_type NOT NULL,
    total_amount NUMERIC(36,12) NOT NULL CHECK(total_amount >= 0),
    remaining_amount NUMERIC(36,12) NOT NULL CHECK(remaining_amount >= 0),
    rollover_policy rollover_policy NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NOT NULL,
    restriction JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP
);
CREATE INDEX idx_commitment_contract ON commitments(contract_id);
ALTER TABLE commitments ENABLE ROW LEVEL SECURITY;

-- 路由规则（任务→模型）
CREATE TABLE route_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    name VARCHAR(255) NOT NULL,
    task_type VARCHAR(255) NOT NULL,
    target_provider VARCHAR(255) NOT NULL,
    target_model VARCHAR(255) NOT NULL,
    fallback_provider VARCHAR(255),
    fallback_model VARCHAR(255),
    policy_config JSONB NOT NULL DEFAULT '{}'::JSONB,
    priority INT NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP
);
CREATE INDEX idx_routerule_tenant_task ON route_rules(tenant_id, task_type);
ALTER TABLE route_rules ENABLE ROW LEVEL SECURITY;

-- 预算池
CREATE TABLE budget_pools (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL REFERENCES customers(id),
    task_type VARCHAR(255),
    agent_id VARCHAR(255),
    max_budget NUMERIC(36,12) NOT NULL CHECK(max_budget >= 0),
    warning_threshold NUMERIC(36,12),
    exhaust_policy exhaust_policy NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    UNIQUE(tenant_id, customer_id, task_type, agent_id)
);
CREATE INDEX idx_budgetpool_tenant_customer ON budget_pools(tenant_id, customer_id, task_type, agent_id);
ALTER TABLE budget_pools ENABLE ROW LEVEL SECURITY;

-- 预算预留表 BudgetReservation
-- 约束：status 禁止业务直接 UPDATE，仅通过 confirm/release/expire 接口流转
CREATE TABLE budget_reservations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL REFERENCES customers(id),
    pool_id UUID NOT NULL REFERENCES budget_pools(id),
    transaction_id UUID NOT NULL,
    estimated_amount NUMERIC(36,12) NOT NULL CHECK(estimated_amount >= 0),
    reserved_amount NUMERIC(36,12) NOT NULL CHECK(reserved_amount >= 0),
    actual_amount NUMERIC(36,12),
    status reservation_status NOT NULL DEFAULT 'pending',
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    confirmed_at TIMESTAMP,
    released_at TIMESTAMP,
    UNIQUE(tenant_id, transaction_id)
);
CREATE INDEX idx_reservation_pool_status ON budget_reservations(pool_id, status, expires_at);
CREATE INDEX idx_reservation_transaction ON budget_reservations(tenant_id, transaction_id);
ALTER TABLE budget_reservations ENABLE ROW LEVEL SECURITY;

-- 账本流水：不可变，禁止UPDATE，调整新增流水
CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL REFERENCES customers(id),
    commitment_id UUID REFERENCES commitments(id),
    pool_id UUID REFERENCES budget_pools(id),
    reservation_id UUID REFERENCES budget_reservations(id),
    amount NUMERIC(36,12) NOT NULL,
    entry_type VARCHAR(30) NOT NULL,
    source_event_id UUID,
    bill_id UUID,
    memo TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_ledger_customer_time ON ledger_entries(tenant_id, customer_id, created_at DESC);
CREATE INDEX idx_ledger_source_event ON ledger_entries(tenant_id, source_event_id);
ALTER TABLE ledger_entries ENABLE ROW LEVEL SECURITY;

-- 账单主表
CREATE TABLE bills (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    customer_id UUID NOT NULL REFERENCES customers(id),
    contract_id UUID NOT NULL REFERENCES contracts(id),
    status bill_status NOT NULL DEFAULT 'draft',
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,
    total_amount NUMERIC(36,12) NOT NULL DEFAULT 0,
    currency VARCHAR(3) NOT NULL,
    pdf_path VARCHAR(512),
    adjustment_bill_id UUID REFERENCES bills(id),
    adjustment_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    finalized_at TIMESTAMP,
    voided_at TIMESTAMP,
    deleted_at TIMESTAMP
);
CREATE INDEX idx_bills_period ON bills(tenant_id, customer_id, period_start, period_end);
CREATE INDEX idx_bills_adjustment ON bills(adjustment_bill_id);
ALTER TABLE bills ENABLE ROW LEVEL SECURITY;

-- 账单行项目
CREATE TABLE bill_line_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    bill_id UUID NOT NULL REFERENCES bills(id),
    product_id UUID NOT NULL REFERENCES products(id),
    amount NUMERIC(36,12) NOT NULL,
    quantity NUMERIC(36,12) NOT NULL,
    pricing_context_snapshot JSONB NOT NULL,
    metadata JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_bill_line_bill ON bill_line_items(bill_id);
ALTER TABLE bill_line_items ENABLE ROW LEVEL SECURITY;

-- BillingRun：账单跑批任务
CREATE TABLE billing_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,
    status billing_run_status NOT NULL DEFAULT 'pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    total_customers INT NOT NULL DEFAULT 0,
    total_bills INT NOT NULL DEFAULT 0,
    error_count INT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    error_detail JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_billing_run_tenant_time ON billing_runs(tenant_id, period_start, period_end);
ALTER TABLE billing_runs ENABLE ROW LEVEL SECURITY;

-- ReconciliationRun：对账跑批任务
CREATE TABLE reconciliation_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    run_date DATE NOT NULL,
    scope JSONB NOT NULL,
    status reconciliation_run_status NOT NULL DEFAULT 'pending',
    usage_total NUMERIC(36,12),
    rated_total NUMERIC(36,12),
    ledger_total NUMERIC(36,12),
    diff_amount NUMERIC(36,12),
    corrected_count INT NOT NULL DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_reconciliation_run_tenant_date ON reconciliation_runs(tenant_id, run_date);
ALTER TABLE reconciliation_runs ENABLE ROW LEVEL SECURITY;

-- Webhook投递记录表
CREATE TABLE webhook_deliveries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    signature_header_value TEXT,
    target_url VARCHAR(1024) NOT NULL,
    status webhook_delivery_status NOT NULL DEFAULT 'pending',
    retry_attempt INT NOT NULL DEFAULT 0,
    max_retry INT NOT NULL DEFAULT 5,
    http_status INT,
    response_body TEXT,
    last_error TEXT,
    next_retry_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_webhook_delivery_tenant_status ON webhook_deliveries(tenant_id, status);
ALTER TABLE webhook_deliveries ENABLE ROW LEVEL SECURITY;
