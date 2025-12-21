// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Benchmark tokenization performance
//!
//! This benchmark measures tokenization speed using a complex SQL query
//! with many identifiers, keywords, string literals, and comments.

use criterion::{criterion_group, criterion_main, Criterion};
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::Tokenizer;

const COMPLEX_SQL: &str = r#"
  -- ============================================================================
  -- Enterprise Sales Analytics Dashboard Query
  -- ============================================================================
  -- Purpose: Comprehensive sales analysis across multiple dimensions
  -- Author: Analytics Team
  -- Last Modified: 2024-01-15
  -- ============================================================================

  /*
   * This query aggregates sales data from multiple sources:
   * - Customer transactions and lifetime value
   * - Product performance across categories
   * - Regional sales trends and patterns
   * - Employee commission calculations
   * - Inventory and fulfillment metrics
   */

  WITH customer_segments AS (
      -- Segment customers by purchase behavior and demographics
      SELECT
          customer_id,
          customer_number,
          customer_name,
          customer_type,
          customer_status,
          customer_tier,
          email_address,
          phone_number,
          mobile_number,
          fax_number,
          date_of_birth,
          registration_date,
          last_login_date,
          account_status,
          email_verified,
          phone_verified,
          -- Address information
          billing_address_line1,
          billing_address_line2,
          billing_city,
          billing_state,
          billing_postal_code,
          billing_country,
          shipping_address_line1,
          shipping_address_line2,
          shipping_city,
          shipping_state,
          shipping_postal_code,
          shipping_country,
          -- Demographics
          gender,
          age_group,
          income_bracket,
          education_level,
          occupation,
          marital_status,
          -- Marketing preferences
          marketing_opt_in,
          sms_opt_in,
          email_frequency,
          preferred_channel,
          preferred_language,
          -- Calculated fields
          CASE
              WHEN customer_status = 'active' AND last_login_date >= CURRENT_DATE - INTERVAL '30' DAY THEN 'highly_active'
              WHEN customer_status = 'active' AND last_login_date >= CURRENT_DATE - INTERVAL '90' DAY THEN 'active'
              WHEN customer_status = 'active' THEN 'inactive'
              ELSE 'dormant'
          END AS activity_level,
          CASE
              WHEN registration_date >= CURRENT_DATE - INTERVAL '1' YEAR THEN 'new'
              WHEN registration_date >= CURRENT_DATE - INTERVAL '3' YEAR THEN 'established'
              ELSE 'veteran'
          END AS customer_tenure
      FROM customers
      WHERE customer_status IN ('active', 'pending', 'suspended')
          AND registration_date >= '2020-01-01'
          AND billing_country IN ('USA', 'Canada', 'Mexico', 'UK', 'Germany', 'France', 'Spain', 'Italy')
          AND email_address NOT LIKE '%@test.com'
          AND email_address NOT LIKE '%@example.com'
          AND customer_name IS NOT NULL
  ),

  product_catalog AS (
      -- Product information with categories and attributes
      SELECT
          product_id,
          product_sku,
          product_name,
          product_description,
          product_category,
          product_subcategory,
          product_brand,
          product_manufacturer,
          product_supplier,
          product_model,
          product_series,
          product_version,
          -- Pricing
          list_price,
          cost_price,
          sale_price,
          wholesale_price,
          minimum_price,
          suggested_retail_price,
          -- Attributes
          product_color,
          product_size,
          product_weight,
          product_length,
          product_width,
          product_height,
          product_material,
          product_warranty,
          -- Inventory
          stock_quantity,
          reorder_level,
          reorder_quantity,
          warehouse_location,
          bin_location,
          aisle_number,
          shelf_number,
          -- Status
          product_status,
          availability_status,
          is_featured,
          is_new_arrival,
          is_on_sale,
          is_clearance,
          is_discontinued,
          launch_date,
          discontinuation_date,
          -- Ratings
          average_rating,
          review_count,
          return_rate,
          defect_rate,
          -- Categories
          CASE
              WHEN product_category = 'electronics' THEN 'high_tech'
              WHEN product_category IN ('clothing', 'shoes', 'accessories') THEN 'fashion'
              WHEN product_category IN ('home', 'garden', 'furniture') THEN 'home_living'
              WHEN product_category IN ('sports', 'outdoor', 'fitness') THEN 'active_lifestyle'
              ELSE 'general_merchandise'
          END AS category_group
      FROM products
      WHERE product_status = 'active'
          AND availability_status IN ('in_stock', 'low_stock', 'backorder')
          AND is_discontinued = FALSE
          AND launch_date <= CURRENT_DATE
  ),

  order_transactions AS (
      -- Order and transaction details
      SELECT
          order_id,
          order_number,
          order_date,
          order_time,
          order_timestamp,
          customer_id,
          order_status,
          order_type,
          order_channel,
          order_source,
          -- Payment information
          payment_method,
          payment_status,
          payment_date,
          payment_reference,
          transaction_id,
          authorization_code,
          -- Financial details
          subtotal_amount,
          tax_amount,
          shipping_amount,
          discount_amount,
          coupon_amount,
          gift_card_amount,
          total_amount,
          paid_amount,
          refund_amount,
          net_amount,
          -- Shipping details
          shipping_method,
          shipping_carrier,
          tracking_number,
          shipped_date,
          estimated_delivery_date,
          actual_delivery_date,
          delivery_status,
          signature_required,
          -- Location
          ship_to_address_line1,
          ship_to_address_line2,
          ship_to_city,
          ship_to_state,
          ship_to_postal_code,
          ship_to_country,
          -- Fulfillment
          warehouse_id,
          fulfillment_center,
          picker_id,
          packer_id,
          shipper_id,
          -- Timestamps
          created_at,
          updated_at,
          completed_at,
          cancelled_at,
          -- Flags
          is_gift,
          is_rush_order,
          is_international,
          requires_signature,
          is_business_order,
          -- Notes
          customer_notes,
          internal_notes,
          gift_message,
          special_instructions
      FROM orders
      WHERE order_date >= '2023-01-01'
          AND order_date < '2024-12-31'
          AND order_status IN ('pending', 'processing', 'shipped', 'delivered', 'completed')
          AND order_type IN ('standard', 'express', 'overnight', 'international')
          AND total_amount > 0
          AND customer_id IS NOT NULL
  ),

  order_line_items AS (
      -- Individual line items from orders
      SELECT
          line_item_id,
          order_id,
          product_id,
          line_number,
          -- Quantities
          quantity_ordered,
          quantity_shipped,
          quantity_cancelled,
          quantity_returned,
          quantity_damaged,
          -- Pricing
          unit_price,
          unit_cost,
          unit_discount,
          line_subtotal,
          line_tax,
          line_shipping,
          line_total,
          -- Discounts
          discount_type,
          discount_code,
          discount_percentage,
          discount_reason,
          -- Product details at time of order
          product_sku_snapshot,
          product_name_snapshot,
          product_category_snapshot,
          -- Status
          line_status,
          fulfillment_status,
          return_status,
          -- Warehouse
          picked_from_warehouse,
          picked_from_location,
          picked_by_user,
          picked_at_timestamp,
          packed_by_user,
          packed_at_timestamp,
          -- Returns
          return_reason,
          return_date,
          refund_amount,
          restocking_fee,
          -- Gift wrap
          is_gift_wrapped,
          gift_wrap_type,
          gift_wrap_charge,
          -- Calculated fields
          unit_price * quantity_ordered AS line_revenue,
          unit_cost * quantity_ordered AS line_cost,
          (unit_price - unit_cost) * quantity_ordered AS line_profit,
          CASE
              WHEN quantity_returned > 0 THEN 'returned'
              WHEN quantity_cancelled > 0 THEN 'cancelled'
              WHEN quantity_shipped = quantity_ordered THEN 'fulfilled'
              ELSE 'partial'
          END AS fulfillment_type
      FROM order_items
      WHERE line_status NOT IN ('cancelled', 'voided')
          AND quantity_ordered > 0
  ),

  employee_data AS (
      -- Employee and sales representative information
      SELECT
          employee_id,
          employee_number,
          employee_name,
          first_name,
          last_name,
          middle_name,
          email_address,
          phone_extension,
          mobile_phone,
          -- Employment details
          hire_date,
          termination_date,
          employment_status,
          employment_type,
          job_title,
          job_level,
          job_grade,
          department_id,
          department_name,
          division_id,
          division_name,
          -- Management
          manager_id,
          manager_name,
          reports_to,
          -- Location
          office_location,
          office_building,
          office_floor,
          office_room,
          work_city,
          work_state,
          work_country,
          -- Compensation
          base_salary,
          commission_rate,
          bonus_target,
          commission_tier,
          -- Performance
          sales_quota,
          current_sales,
          quota_attainment,
          performance_rating,
          last_review_date,
          next_review_date
      FROM employees
      WHERE employment_status = 'active'
          AND employee_id IS NOT NULL
          AND hire_date <= CURRENT_DATE
  ),

  customer_lifetime_metrics AS (
      -- Calculate customer lifetime value and metrics
      SELECT
          cs.customer_id,
          cs.customer_name,
          cs.customer_tier,
          cs.activity_level,
          -- Order counts
          COUNT(DISTINCT ot.order_id) AS total_orders,
          COUNT(DISTINCT CASE WHEN ot.order_date >= CURRENT_DATE - INTERVAL '30' DAY THEN ot.order_id END) AS orders_last_30_days,
          COUNT(DISTINCT CASE WHEN ot.order_date >= CURRENT_DATE - INTERVAL '90' DAY THEN ot.order_id END) AS orders_last_90_days,
          COUNT(DISTINCT CASE WHEN ot.order_date >= CURRENT_DATE - INTERVAL '365' DAY THEN ot.order_id END) AS orders_last_year,
          -- Revenue metrics
          SUM(ot.total_amount) AS lifetime_revenue,
          SUM(CASE WHEN ot.order_date >= CURRENT_DATE - INTERVAL '30' DAY THEN ot.total_amount ELSE 0 END) AS revenue_last_30_days,
          SUM(CASE WHEN ot.order_date >= CURRENT_DATE - INTERVAL '90' DAY THEN ot.total_amount ELSE 0 END) AS revenue_last_90_days,
          SUM(CASE WHEN ot.order_date >= CURRENT_DATE - INTERVAL '365' DAY THEN ot.total_amount ELSE 0 END) AS revenue_last_year,
          -- Average values
          AVG(ot.total_amount) AS average_order_value,
          AVG(CASE WHEN ot.order_date >= CURRENT_DATE - INTERVAL '365' DAY THEN ot.total_amount END) AS avg_order_value_last_year,
          -- Product metrics
          COUNT(DISTINCT oli.product_id) AS unique_products_purchased,
          SUM(oli.quantity_ordered) AS total_items_purchased,
          -- Return metrics
          SUM(oli.quantity_returned) AS total_items_returned,
          SUM(CASE WHEN oli.quantity_returned > 0 THEN oli.refund_amount ELSE 0 END) AS total_refund_amount,
          -- Date ranges
          MIN(ot.order_date) AS first_order_date,
          MAX(ot.order_date) AS last_order_date,
          MAX(ot.order_date) - MIN(ot.order_date) AS customer_lifespan_days,
          -- Recency
          CURRENT_DATE - MAX(ot.order_date) AS days_since_last_order
      FROM customer_segments cs
      LEFT JOIN order_transactions ot ON cs.customer_id = ot.customer_id
      LEFT JOIN order_line_items oli ON ot.order_id = oli.order_id
      WHERE ot.order_status IN ('delivered', 'completed')
      GROUP BY
          cs.customer_id,
          cs.customer_name,
          cs.customer_tier,
          cs.activity_level
  ),

  product_performance AS (
      -- Product sales performance metrics
      SELECT
          pc.product_id,
          pc.product_sku,
          pc.product_name,
          pc.product_category,
          pc.product_subcategory,
          pc.product_brand,
          pc.category_group,
          -- Sales metrics
          COUNT(DISTINCT oli.order_id) AS total_orders,
          SUM(oli.quantity_ordered) AS total_quantity_sold,
          SUM(oli.quantity_returned) AS total_quantity_returned,
          SUM(oli.line_revenue) AS total_revenue,
          SUM(oli.line_cost) AS total_cost,
          SUM(oli.line_profit) AS total_profit,
          -- Averages
          AVG(oli.unit_price) AS average_selling_price,
          AVG(oli.line_revenue) AS average_line_revenue,
          -- Return rate
          CAST(SUM(oli.quantity_returned) AS DECIMAL) / NULLIF(SUM(oli.quantity_ordered), 0) AS return_rate,
          -- Profit margin
          CAST(SUM(oli.line_profit) AS DECIMAL) / NULLIF(SUM(oli.line_revenue), 0) AS profit_margin,
          -- Rankings
          RANK() OVER (PARTITION BY pc.product_category ORDER BY SUM(oli.line_revenue) DESC) AS revenue_rank_in_category,
          RANK() OVER (ORDER BY SUM(oli.quantity_ordered) DESC) AS quantity_rank_overall
      FROM product_catalog pc
      INNER JOIN order_line_items oli ON pc.product_id = oli.product_id
      INNER JOIN order_transactions ot ON oli.order_id = ot.order_id
      WHERE ot.order_status IN ('delivered', 'completed')
          AND ot.order_date >= '2023-01-01'
      GROUP BY
          pc.product_id,
          pc.product_sku,
          pc.product_name,
          pc.product_category,
          pc.product_subcategory,
          pc.product_brand,
          pc.category_group
  ),

  regional_sales AS (
      -- Sales performance by region
      SELECT
          cs.billing_country,
          cs.billing_state,
          cs.billing_city,
          -- Order metrics
          COUNT(DISTINCT ot.order_id) AS total_orders,
          COUNT(DISTINCT cs.customer_id) AS unique_customers,
          -- Revenue
          SUM(ot.total_amount) AS total_revenue,
          SUM(ot.shipping_amount) AS total_shipping_revenue,
          SUM(ot.tax_amount) AS total_tax_collected,
          AVG(ot.total_amount) AS average_order_value,
          -- Time periods
          SUM(CASE WHEN ot.order_date >= '2024-01-01' THEN ot.total_amount ELSE 0 END) AS revenue_2024,
          SUM(CASE WHEN ot.order_date >= '2023-01-01' AND ot.order_date < '2024-01-01' THEN ot.total_amount ELSE 0 END) AS revenue_2023,
          -- Growth
          (SUM(CASE WHEN ot.order_date >= '2024-01-01' THEN ot.total_amount ELSE 0 END) -
           SUM(CASE WHEN ot.order_date >= '2023-01-01' AND ot.order_date < '2024-01-01' THEN ot.total_amount ELSE 0 END)) /
          NULLIF(SUM(CASE WHEN ot.order_date >= '2023-01-01' AND ot.order_date < '2024-01-01' THEN ot.total_amount ELSE 0 END), 0) AS year_over_year_growth
      FROM customer_segments cs
      INNER JOIN order_transactions ot ON cs.customer_id = ot.customer_id
      WHERE ot.order_status IN ('delivered', 'completed')
      GROUP BY
          cs.billing_country,
          cs.billing_state,
          cs.billing_city
      HAVING SUM(ot.total_amount) > 1000
  ),

  monthly_trends AS (
      -- Monthly sales trends and seasonality
      SELECT
          DATE_TRUNC('month', ot.order_date) AS order_month,
          EXTRACT(YEAR FROM ot.order_date) AS order_year,
          EXTRACT(MONTH FROM ot.order_date) AS month_number,
          EXTRACT(QUARTER FROM ot.order_date) AS quarter_number,
          -- Volume metrics
          COUNT(DISTINCT ot.order_id) AS orders,
          COUNT(DISTINCT ot.customer_id) AS customers,
          SUM(oli.quantity_ordered) AS items_sold,
          -- Financial metrics
          SUM(ot.subtotal_amount) AS subtotal,
          SUM(ot.tax_amount) AS tax,
          SUM(ot.shipping_amount) AS shipping,
          SUM(ot.discount_amount) AS discounts,
          SUM(ot.total_amount) AS revenue,
          -- Averages
          AVG(ot.total_amount) AS avg_order_value,
          AVG(oli.quantity_ordered) AS avg_items_per_order,
          -- Moving averages
          AVG(SUM(ot.total_amount)) OVER (ORDER BY DATE_TRUNC('month', ot.order_date) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS three_month_moving_avg,
          AVG(SUM(ot.total_amount)) OVER (ORDER BY DATE_TRUNC('month', ot.order_date) ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS six_month_moving_avg
      FROM order_transactions ot
      INNER JOIN order_line_items oli ON ot.order_id = oli.order_id
      WHERE ot.order_status IN ('delivered', 'completed')
          AND ot.order_date >= '2022-01-01'
      GROUP BY
          DATE_TRUNC('month', ot.order_date),
          EXTRACT(YEAR FROM ot.order_date),
          EXTRACT(MONTH FROM ot.order_date),
          EXTRACT(QUARTER FROM ot.order_date)
  ),

  category_analysis AS (
      -- Category performance analysis
      SELECT
          pc.product_category,
          pc.product_subcategory,
          pc.category_group,
          -- Sales
          COUNT(DISTINCT oli.order_id) AS orders,
          SUM(oli.quantity_ordered) AS quantity,
          SUM(oli.line_revenue) AS revenue,
          SUM(oli.line_profit) AS profit,
          -- Market share
          SUM(oli.line_revenue) / SUM(SUM(oli.line_revenue)) OVER () AS revenue_share,
          -- Pricing
          AVG(oli.unit_price) AS avg_price,
          MIN(oli.unit_price) AS min_price,
          MAX(oli.unit_price) AS max_price,
          -- Profitability
          SUM(oli.line_profit) / NULLIF(SUM(oli.line_revenue), 0) AS profit_margin,
          -- Returns
          SUM(oli.quantity_returned) AS returns,
          CAST(SUM(oli.quantity_returned) AS DECIMAL) / NULLIF(SUM(oli.quantity_ordered), 0) AS return_rate
      FROM product_catalog pc
      INNER JOIN order_line_items oli ON pc.product_id = oli.product_id
      INNER JOIN order_transactions ot ON oli.order_id = ot.order_id
      WHERE ot.order_status IN ('delivered', 'completed')
      GROUP BY
          pc.product_category,
          pc.product_subcategory,
          pc.category_group
  )

  -- Main query combining all CTEs
  SELECT
      -- Customer information
      cs.customer_id,
      cs.customer_number,
      cs.customer_name,
      cs.customer_type,
      cs.customer_tier,
      cs.activity_level,
      cs.customer_tenure,
      cs.email_address,
      cs.phone_number,
      cs.billing_city,
      cs.billing_state,
      cs.billing_country,
      cs.age_group,
      cs.gender,
      cs.income_bracket,
      -- Customer metrics
      clm.total_orders,
      clm.orders_last_30_days,
      clm.orders_last_90_days,
      clm.orders_last_year,
      clm.lifetime_revenue,
      clm.revenue_last_30_days,
      clm.revenue_last_90_days,
      clm.revenue_last_year,
      clm.average_order_value,
      clm.unique_products_purchased,
      clm.total_items_purchased,
      clm.total_items_returned,
      clm.first_order_date,
      clm.last_order_date,
      clm.days_since_last_order,
      -- Order details
      ot.order_id,
      ot.order_number,
      ot.order_date,
      ot.order_status,
      ot.order_type,
      ot.order_channel,
      ot.payment_method,
      ot.payment_status,
      ot.subtotal_amount,
      ot.tax_amount,
      ot.shipping_amount,
      ot.discount_amount,
      ot.total_amount,
      ot.shipping_method,
      ot.shipping_carrier,
      ot.tracking_number,
      ot.delivery_status,
      -- Line item details
      oli.line_item_id,
      oli.product_id,
      oli.quantity_ordered,
      oli.quantity_shipped,
      oli.unit_price,
      oli.line_total,
      oli.discount_type,
      oli.line_status,
      -- Product information
      pc.product_sku,
      pc.product_name,
      pc.product_category,
      pc.product_subcategory,
      pc.product_brand,
      pc.product_manufacturer,
      pc.category_group,
      pc.list_price,
      pc.product_color,
      pc.product_size,
      pc.average_rating,
      pc.review_count,
      -- Product performance
      pp.total_quantity_sold AS product_total_quantity_sold,
      pp.total_revenue AS product_total_revenue,
      pp.total_profit AS product_total_profit,
      pp.return_rate AS product_return_rate,
      pp.profit_margin AS product_profit_margin,
      pp.revenue_rank_in_category,
      -- Employee information
      ed.employee_id,
      ed.employee_name,
      ed.job_title,
      ed.department_name,
      ed.office_location,
      ed.commission_rate,
      ed.sales_quota,
      -- Regional metrics
      rs.total_orders AS region_total_orders,
      rs.unique_customers AS region_unique_customers,
      rs.total_revenue AS region_total_revenue,
      rs.average_order_value AS region_avg_order_value,
      rs.year_over_year_growth AS region_yoy_growth,
      -- Category metrics
      ca.revenue AS category_revenue,
      ca.profit AS category_profit,
      ca.revenue_share AS category_revenue_share,
      ca.profit_margin AS category_profit_margin,
      ca.return_rate AS category_return_rate,
      -- Monthly trends
      mt.order_month,
      mt.three_month_moving_avg,
      mt.six_month_moving_avg,
      -- Calculated fields
      CASE
          WHEN clm.lifetime_revenue > 10000 THEN 'vip'
          WHEN clm.lifetime_revenue > 5000 THEN 'premium'
          WHEN clm.lifetime_revenue > 1000 THEN 'standard'
          ELSE 'basic'
      END AS calculated_tier,
      CASE
          WHEN clm.days_since_last_order <= 30 THEN 'very_recent'
          WHEN clm.days_since_last_order <= 90 THEN 'recent'
          WHEN clm.days_since_last_order <= 180 THEN 'moderate'
          ELSE 'at_risk'
      END AS recency_segment,
      CASE
          WHEN clm.total_orders >= 50 THEN 'frequent'
          WHEN clm.total_orders >= 20 THEN 'regular'
          WHEN clm.total_orders >= 5 THEN 'occasional'
          ELSE 'rare'
      END AS frequency_segment,
      oli.unit_price * oli.quantity_ordered AS calculated_line_revenue,
      (oli.unit_price * oli.quantity_ordered) * (ed.commission_rate / 100) AS calculated_commission,
      ROUND(oli.unit_price * oli.quantity_ordered * 0.9, 2) AS discounted_line_total,
      -- Window functions
      ROW_NUMBER() OVER (PARTITION BY cs.customer_id ORDER BY ot.order_date DESC) AS order_recency_rank,
      RANK() OVER (PARTITION BY cs.billing_country ORDER BY clm.lifetime_revenue DESC) AS customer_value_rank_in_country,
      DENSE_RANK() OVER (PARTITION BY pc.product_category ORDER BY oli.quantity_ordered DESC) AS product_popularity_rank,
      SUM(ot.total_amount) OVER (PARTITION BY cs.customer_id ORDER BY ot.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_customer_revenue,
      AVG(ot.total_amount) OVER (PARTITION BY cs.customer_id ORDER BY ot.order_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_5_order_avg,
      -- Aggregates
      SUM(oli.quantity_ordered) OVER (PARTITION BY pc.product_category) AS category_total_quantity,
      COUNT(DISTINCT ot.order_id) OVER (PARTITION BY cs.billing_country, DATE_TRUNC('month', ot.order_date)) AS monthly_orders_in_country,
      MAX(ot.total_amount) OVER (PARTITION BY cs.customer_id) AS customer_largest_order,
      MIN(ot.order_date) OVER (PARTITION BY pc.product_id) AS product_first_sale_date

  FROM customer_segments cs
  INNER JOIN customer_lifetime_metrics clm ON cs.customer_id = clm.customer_id
  INNER JOIN order_transactions ot ON cs.customer_id = ot.customer_id
  INNER JOIN order_line_items oli ON ot.order_id = oli.order_id
  INNER JOIN product_catalog pc ON oli.product_id = pc.product_id
  INNER JOIN product_performance pp ON pc.product_id = pp.product_id
  LEFT JOIN employee_data ed ON ot.order_id IN (
      SELECT order_id FROM employee_assignments WHERE employee_id = ed.employee_id
  )
  LEFT JOIN regional_sales rs ON cs.billing_country = rs.billing_country
      AND cs.billing_state = rs.billing_state
      AND cs.billing_city = rs.billing_city
  LEFT JOIN category_analysis ca ON pc.product_category = ca.product_category
      AND pc.product_subcategory = ca.product_subcategory
  LEFT JOIN monthly_trends mt ON DATE_TRUNC('month', ot.order_date) = mt.order_month

  WHERE
      -- Date filters
      ot.order_date >= '2023-01-01'
      AND ot.order_date < '2024-12-31'
      -- Status filters
      AND ot.order_status IN ('processing', 'shipped', 'delivered', 'completed')
      AND oli.line_status NOT IN ('cancelled', 'voided', 'rejected')
      AND cs.customer_status = 'active'
      AND pc.product_status = 'active'
      -- Geographic filters
      AND cs.billing_country IN ('USA', 'Canada', 'Mexico', 'UK', 'Germany', 'France', 'Spain', 'Italy', 'Japan', 'Australia')
      AND cs.billing_state NOT IN ('test', 'demo', 'internal')
      -- Category filters
      AND pc.product_category IN ('electronics', 'clothing', 'home', 'sports', 'books', 'toys', 'automotive', 'health', 'beauty', 'grocery')
      AND pc.product_subcategory NOT LIKE '%test%'
      -- Amount filters
      AND ot.total_amount > 0
      AND ot.total_amount < 100000
      AND oli.quantity_ordered > 0
      AND oli.unit_price > 0
      -- Quality filters
      AND cs.email_address NOT LIKE '%@test.com'
      AND cs.email_address NOT LIKE '%@example.com'
      AND cs.email_address NOT LIKE '%@invalid.com'
      AND cs.customer_name NOT LIKE '%test%'
      AND cs.customer_name NOT LIKE '%demo%'
      AND pc.product_name NOT LIKE '%sample%'
      AND pc.product_name NOT LIKE '%demo%'
      -- Tier filters
      AND cs.customer_tier IN ('gold', 'silver', 'bronze', 'platinum')
      AND cs.activity_level IN ('highly_active', 'active')
      -- Payment filters
      AND ot.payment_status = 'completed'
      AND ot.payment_method IN ('credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer')
      -- Shipping filters
      AND ot.delivery_status IN ('delivered', 'in_transit', 'out_for_delivery')
      AND ot.shipping_method IN ('standard', 'express', 'overnight', 'two_day')
      -- Channel filters
      AND ot.order_channel IN ('web', 'mobile', 'tablet', 'phone', 'store', 'marketplace')
      -- Null checks
      AND cs.customer_id IS NOT NULL
      AND ot.order_id IS NOT NULL
      AND oli.product_id IS NOT NULL
      AND pc.product_sku IS NOT NULL
      AND ot.total_amount IS NOT NULL

  GROUP BY
      cs.customer_id, cs.customer_number, cs.customer_name, cs.customer_type, cs.customer_tier,
      cs.activity_level, cs.customer_tenure, cs.email_address, cs.phone_number,
      cs.billing_city, cs.billing_state, cs.billing_country, cs.age_group, cs.gender, cs.income_bracket,
      clm.total_orders, clm.orders_last_30_days, clm.orders_last_90_days, clm.orders_last_year,
      clm.lifetime_revenue, clm.revenue_last_30_days, clm.revenue_last_90_days, clm.revenue_last_year,
      clm.average_order_value, clm.unique_products_purchased, clm.total_items_purchased,
      clm.total_items_returned, clm.first_order_date, clm.last_order_date, clm.days_since_last_order,
      ot.order_id, ot.order_number, ot.order_date, ot.order_status, ot.order_type, ot.order_channel,
      ot.payment_method, ot.payment_status, ot.subtotal_amount, ot.tax_amount, ot.shipping_amount,
      ot.discount_amount, ot.total_amount, ot.shipping_method, ot.shipping_carrier, ot.tracking_number,
      ot.delivery_status, oli.line_item_id, oli.product_id, oli.quantity_ordered, oli.quantity_shipped,
      oli.unit_price, oli.line_total, oli.discount_type, oli.line_status,
      pc.product_sku, pc.product_name, pc.product_category, pc.product_subcategory, pc.product_brand,
      pc.product_manufacturer, pc.category_group, pc.list_price, pc.product_color, pc.product_size,
      pc.average_rating, pc.review_count, pp.total_quantity_sold, pp.total_revenue, pp.total_profit,
      pp.return_rate, pp.profit_margin, pp.revenue_rank_in_category,
      ed.employee_id, ed.employee_name, ed.job_title, ed.department_name, ed.office_location,
      ed.commission_rate, ed.sales_quota, rs.total_orders, rs.unique_customers, rs.total_revenue,
      rs.average_order_value, rs.year_over_year_growth,
      ca.revenue, ca.profit, ca.revenue_share, ca.profit_margin, ca.return_rate,
      mt.order_month, mt.three_month_moving_avg, mt.six_month_moving_avg

  HAVING
      SUM(oli.quantity_ordered) > 0
      AND SUM(oli.line_total) > 0
      AND COUNT(DISTINCT ot.order_id) >= 1

  ORDER BY
      clm.lifetime_revenue DESC,
      clm.total_orders DESC,
      ot.order_date DESC,
      cs.customer_name ASC,
      pc.product_category ASC,
      pc.product_name ASC,
      oli.line_number ASC,
      ot.order_id ASC

  LIMIT 100000
  OFFSET 0;

  -- Additional analytics queries for dashboard

  -- Top customers by revenue
  SELECT
      customer_id,
      customer_name,
      customer_tier,
      total_orders,
      lifetime_revenue,
      average_order_value,
      days_since_last_order
  FROM customer_lifetime_metrics
  WHERE lifetime_revenue > 1000
  ORDER BY lifetime_revenue DESC
  LIMIT 100;

  -- Top products by sales
  SELECT
      product_sku,
      product_name,
      product_category,
      product_brand,
      total_quantity_sold,
      total_revenue,
      total_profit,
      profit_margin,
      return_rate
  FROM product_performance
  WHERE total_revenue > 5000
  ORDER BY total_revenue DESC
  LIMIT 50;

  -- Regional performance summary
  SELECT
      billing_country,
      billing_state,
      total_orders,
      unique_customers,
      total_revenue,
      average_order_value,
      year_over_year_growth
  FROM regional_sales
  WHERE total_revenue > 10000
  ORDER BY total_revenue DESC;
"#;

fn tokenization_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("tokenization");
    let dialect = GenericDialect {};

    group.bench_function("tokenize_complex_sql", |b| {
        b.iter(|| {
            let mut tokenizer = Tokenizer::new(&dialect, COMPLEX_SQL);
            tokenizer.tokenize().unwrap()
        });
    });

    group.finish();
}

criterion_group!(benches, tokenization_benchmark);
criterion_main!(benches);
