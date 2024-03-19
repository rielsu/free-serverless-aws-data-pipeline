employees_hired_per_job_2021 = """
    SELECT
        d.department,
        j.job,
        COUNT(CASE WHEN EXTRACT(QUARTER FROM CAST(CASE WHEN datetime <> '' THEN from_iso8601_timestamp(datetime) END AS timestamp)) = 1 THEN 1 END) AS Q1,
        COUNT(CASE WHEN EXTRACT(QUARTER FROM CAST(CASE WHEN datetime <> '' THEN from_iso8601_timestamp(datetime) END AS timestamp)) = 2 THEN 1 END) AS Q2,
        COUNT(CASE WHEN EXTRACT(QUARTER FROM CAST(CASE WHEN datetime <> '' THEN from_iso8601_timestamp(datetime) END AS timestamp)) = 3 THEN 1 END) AS Q3,
        COUNT(CASE WHEN EXTRACT(QUARTER FROM CAST(CASE WHEN datetime <> '' THEN from_iso8601_timestamp(datetime) END AS timestamp)) = 4 THEN 1 END) AS Q4
    FROM
        hired_employees he
    JOIN departments d ON he.department_id = d.id
    JOIN jobs j ON he.job_id = j.id
    WHERE EXTRACT(YEAR FROM CAST(CASE WHEN he.datetime <> '' THEN from_iso8601_timestamp(he.datetime) END AS timestamp)) = 2021
    GROUP BY
        d.department,
        j.job
    ORDER BY
        d.department,
        j.job
"""

over_mean_employees_hired_per_department = """
    WITH DepartmentHires AS (
        SELECT
            d.id AS department_id,
            d.department,
            COUNT(he.id) AS hired
        FROM
            departments d
            JOIN hired_employees he ON d.id = he.department_id
        WHERE
            EXTRACT(YEAR FROM CAST(CASE WHEN he.datetime <> '' THEN from_iso8601_timestamp(he.datetime) END AS timestamp)) = 2021
        GROUP BY
            d.id,
            d.department
    ),
    MeanHires AS (
        SELECT
            AVG(hired) AS mean_hired
        FROM
            DepartmentHires
    )
    SELECT
        dh.department_id,
        dh.department,
        dh.hired
    FROM
        DepartmentHires dh,
        MeanHires mh
    WHERE
        dh.hired > mh.mean_hired
    ORDER BY
    dh.hired DESC;
"""
