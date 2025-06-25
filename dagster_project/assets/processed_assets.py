from dagster import asset

@asset(required_resource_keys={"csv_writer"})
def avg_salary_by_department(context, employees, departments):
    # Rename to avoid conflict with employee 'name' column
    departments = departments.rename(columns={"id": "department_id", "name": "department_name"})
    
    # Merge on department_id
    df = employees.merge(departments, on="department_id")

    # Calculate average salary by department
    result = (
        df.groupby("department_name")["salary"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"salary": "avg_salary"})
    )

    # Write to CSV
    context.resources.csv_writer(result, "top_departments.csv")

    return result