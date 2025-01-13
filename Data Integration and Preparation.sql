
-- Approach:
-- Constructs the foundational customer profile by integrating engagement metrics (e.g., number of service calls and successful resolutions).
-- Ensures temporal accuracy by aggregating data at the monthly snapshot level (MONTH_SQN).

-- Step 1: Create the final table directly using CTEs
-- Derive engagement patterns: number of service calls and successful service calls
WITH customer_service_call_result AS (
    SELECT 
        "MONTH_SQN",
        "CUSTOMER_SQN",
        -- Count of distinct service calls made by the customer
        COUNT(DISTINCT "SR_Number") AS "Number_of_Service",
        -- Count of successful service calls based on specific success criteria
        COUNT(DISTINCT CASE 
                            WHEN "REQ_STATUS" IN ('Cleared', 'Closed') THEN "SR_Number"
                            WHEN "REQ_STATUS" = 'Processing Orders' AND "REQ_SUBSTATUS" IN ('Approved', 'SPP Order Created') THEN "SR_Number"
                            WHEN "REQ_STATUS" = 'Submitted' AND "REQ_SUBSTATUS" = 'Approved' THEN "SR_Number"
                            ELSE NULL
                       END) AS "Number_of_Success"
    FROM public."Service_Call"
    GROUP BY 
        "MONTH_SQN",
        "CUSTOMER_SQN"
),

-- Derive customer lifecycle and subscription statuses
customer_lifecycle AS (
    -- Step 2.1: Determine subscription-level statuses
    WITH Subscription_Status AS (
        SELECT 
            "MONTH_SQN",
            "CUSTOMER_SQN",
            "SUBSCRIPTION_SQN",
            "SERVICE",
            CASE 
                WHEN "CHURN_FLAG" = 1 THEN 'Churn'
                WHEN "RECONTRACT_FLAG" = 1 AND "CONTRACT_STATUS" IN ('In Contract', 'Out Of Contract') THEN 'Retention'
                WHEN "NEW_SIGNUP_FLAG" = 1 THEN 'New'
                WHEN "CONTRACT_STATUS" IN ('In Contract', 'Out Of Contract') AND "SUBSCRIPTION_STATUS" = 'Active' THEN 'Engagement'
                WHEN "CONTRACT_STATUS" = 'Out Of Contract' AND "SUBSCRIPTION_STATUS" IN ('Inactive', 'Suspended') THEN 'Expired'
                WHEN "CONTRACT_STATUS" = 'No Contract' THEN 'Lead'
                ELSE 'Unknown'
            END AS "Subscription_Status"
        FROM public."Subscription"
    ),
    -- Step 2.2: Roll up to determine customer lifecycle based on subscription statuses
    Customer_Lifecycle AS (
        SELECT 
            "MONTH_SQN",
            "CUSTOMER_SQN",
            CASE 
                WHEN 'Churn' = ANY(array_agg("Subscription_Status")) THEN 'Churn'
                WHEN 'New' = ANY(array_agg("Subscription_Status")) THEN 'New'
                WHEN 'Retention' = ANY(array_agg("Subscription_Status")) THEN 'Retention'
                WHEN 'Engagement' = ANY(array_agg("Subscription_Status")) THEN 'Engagement'
                WHEN 'Expired' = ANY(array_agg("Subscription_Status")) THEN 'Expired'
                WHEN 'Lead' = ANY(array_agg("Subscription_Status")) THEN 'Lead'
                ELSE 'Unknown'
            END AS "Customer_Lifecycle"
        FROM Subscription_Status
        GROUP BY "MONTH_SQN", "CUSTOMER_SQN"
    )
    SELECT 
        s."MONTH_SQN",
        s."CUSTOMER_SQN",
        s."SUBSCRIPTION_SQN",
        s."SERVICE",
        c."Customer_Lifecycle",
        s."Subscription_Status"
    FROM Subscription_Status s
    LEFT JOIN Customer_Lifecycle c
    ON s."MONTH_SQN" = c."MONTH_SQN" AND s."CUSTOMER_SQN" = c."CUSTOMER_SQN"
),

-- Derive subscription and customer value: active subscription days
customer_value AS (
    -- Step 3.1: Calculate subscription-level value (active days within the contract period)
    WITH Subscription_Days AS (
        SELECT 
            "MONTH_SQN",
            "CUSTOMER_SQN",
            "SUBSCRIPTION_SQN",
            "SERVICE",
            GREATEST(
                "CONTRACT_END_DATE" - "CONTRACT_START_DATE" + 1,
                0
            ) AS "Subscription_Value"
        FROM public."Subscription"
    ),
    -- Step 3.2: Aggregate subscription values to calculate customer-level value
    Customer_Days AS (
        SELECT 
            "MONTH_SQN",
            "CUSTOMER_SQN",
            SUM("Subscription_Value") AS "Customer_Value"
        FROM Subscription_Days
        GROUP BY "MONTH_SQN", "CUSTOMER_SQN"
    )
    SELECT 
        sd."MONTH_SQN",
        sd."CUSTOMER_SQN",
        sd."SUBSCRIPTION_SQN",
        sd."SERVICE",
        sd."Subscription_Value",
        cd."Customer_Value"
    FROM Subscription_Days sd
    LEFT JOIN Customer_Days cd
    ON sd."MONTH_SQN" = cd."MONTH_SQN" AND sd."CUSTOMER_SQN" = cd."CUSTOMER_SQN"
),

-- Derive unique subscription campaign mapping
customer_subscription_campaign_unique AS (
    SELECT 
        "MONTH_SQN",
        "CUSTOMER_SQN",
        "SUBSCRIPTION_SQN",
        "SERVICE",
        "CAMPAIGN_CODE",
        "CAMPAIGN_NAME"
    FROM public."Campaign"
    GROUP BY 
        "MONTH_SQN",
        "CUSTOMER_SQN",
        "SUBSCRIPTION_SQN",
        "SERVICE",
        "CAMPAIGN_CODE",
        "CAMPAIGN_NAME"
)

-- Step 5: Combine all derived features into the final table
SELECT 
    c."MONTH_SQN",
    c."CUSTOMER_SQN",
    c."GENDER", 
    c."AGE", 
    c."RACE", 
    c."NATIONALITY", 
    c."CREDIT_RATING",
    COALESCE(cs."Number_of_Service", 0) AS "Number_of_Service",
    COALESCE(cs."Number_of_Success", 0) AS "Number_of_Success",
    COALESCE(cl."Customer_Lifecycle", 'Unknown') AS "Customer_Lifecycle",
    COALESCE(cv."Customer_Value", 0) AS "Customer_Value",
    cl."SUBSCRIPTION_SQN",
    cl."SERVICE",
    COALESCE(cl."Subscription_Status", 'Unknown') AS "Subscription_Status",
    COALESCE(cv."Subscription_Value", 0) AS "Subscription_Value",
    COALESCE(csc."CAMPAIGN_CODE", 'NA') AS "CAMPAIGN_CODE",
    COALESCE(csc."CAMPAIGN_NAME", 'NA') AS "CAMPAIGN_NAME"
FROM (
    SELECT 
        "MONTH_SQN", 
        "CUSTOMER_SQN", 
        "GENDER", 
        "AGE", 
        "RACE", 
        "NATIONALITY", 
        "CREDIT_RATING"
    FROM public."Customer"
) AS c
LEFT OUTER JOIN customer_service_call_result cs
ON c."CUSTOMER_SQN" = cs."CUSTOMER_SQN" 
   AND c."MONTH_SQN" = cs."MONTH_SQN"
LEFT OUTER JOIN customer_lifecycle cl
ON c."CUSTOMER_SQN" = cl."CUSTOMER_SQN"
   AND c."MONTH_SQN" = cl."MONTH_SQN"
LEFT OUTER JOIN customer_value cv
ON cl."SUBSCRIPTION_SQN" = cv."SUBSCRIPTION_SQN"
   AND cl."SERVICE" = cv."SERVICE"
   AND cl."CUSTOMER_SQN" = cv."CUSTOMER_SQN"
   AND cl."MONTH_SQN" = cv."MONTH_SQN"
LEFT OUTER JOIN customer_subscription_campaign_unique csc
ON cl."SUBSCRIPTION_SQN" = csc."SUBSCRIPTION_SQN"
   AND cl."SERVICE" = csc."SERVICE"
   AND cl."CUSTOMER_SQN" = csc."CUSTOMER_SQN"
   AND cl."MONTH_SQN" = csc."MONTH_SQN";

-- Display the first 10 rows of the final table
SELECT * 
FROM Starhub_Casestudy_db 
LIMIT 10;
