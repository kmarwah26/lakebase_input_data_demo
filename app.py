import psycopg
from databricks.sdk import WorkspaceClient
import uuid
import streamlit as st
import datetime

CATALOG_NAME = "pet_data"
SCHEMA_NAME = "public"
TABLE_NAME = "pet_records"
INSTANCE_NAME = "lakebase-app-demo-instance"


def _get_connection():
    """Initialize connection to Lakebase using Databricks SDK."""
    if "db_conn" not in st.session_state:
        # Initialize the Databricks SDK WorkspaceClient
        w = WorkspaceClient()
        
        # Obtain a temporary connection credential
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[INSTANCE_NAME]
        )
        
        current_user = w.current_user.me().user_name
        instance = w.database.get_database_instance(name=INSTANCE_NAME)
        
        # Connection parameters
        st.session_state.db_conn = psycopg.connect(
            host=instance.read_write_dns,
            dbname="databricks_postgres",
            user=current_user,
            password=cred.token,
            sslmode="require"
        )
    
    return st.session_state.db_conn


def _qualified_table_name() -> str:
    return f'"{SCHEMA_NAME}"."{TABLE_NAME}"'


def _generate_pet_id() -> str:
    return f"PET-{uuid.uuid4().hex[:10].upper()}"


def _create_schema_and_table() -> None:
    conn = _get_connection()
    conn.rollback()  # Clear any failed transaction state
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {_qualified_table_name()} (
            id SERIAL PRIMARY KEY,
            full_name TEXT NOT NULL,
            Pet_id TEXT,
            date_of_birth DATE NOT NULL,
            phone TEXT,
            email TEXT,
            address TEXT,
            visit_date DATE,
            department TEXT,
            symptoms TEXT,
            allergies TEXT,
            additional_notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """

    with conn.cursor() as cur:
        cur.execute(create_table_sql)
    conn.commit()


def insert_Pet_intake(payload: dict) -> None:
    conn = _get_connection()
    _create_schema_and_table()

    insert_sql = f"""
        INSERT INTO {_qualified_table_name()} (
            full_name,
            Pet_id,
            date_of_birth,
            phone,
            email,
            address,
            visit_date,
            department,
            symptoms,
            allergies,
            additional_notes
        ) VALUES (
            %(full_name)s,
            %(Pet_id)s,
            %(date_of_birth)s,
            %(phone)s,
            %(email)s,
            %(address)s,
            %(visit_date)s,
            %(department)s,
            %(symptoms)s,
            %(allergies)s,
            %(additional_notes)s
        );
    """

    with conn.cursor() as cur:
        cur.execute(insert_sql, payload)
    conn.commit()


def fetch_Pet_records() -> list[dict]:
    conn = _get_connection()
    _create_schema_and_table()

    select_sql = f"""
        SELECT
            id,
            full_name,
            Pet_id,
            date_of_birth,
            phone,
            email,
            address,
            visit_date,
            department,
            symptoms,
            allergies,
            additional_notes,
            created_at
        FROM {_qualified_table_name()}
        ORDER BY created_at DESC;
    """

    with conn.cursor() as cur:
        cur.execute(select_sql)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    
    return [dict(zip(columns, row)) for row in rows]


def main() -> None:
    st.set_page_config(page_title="Banfield Pet Hospital Intake", page_icon="🏥")
    st.title("Banfield Pet Hospital Intake Form")
    st.caption("Enter Pet details to create a new intake record.")

    intake_tab, records_tab = st.tabs(["Pet Intake", "Pet Records"])

    with intake_tab:
        with st.form("Pet_intake_form"):
            st.subheader("Pet Details")
            full_name = st.text_input("Full name", placeholder="Jane Doe")
            if "pet_id" not in st.session_state:
                st.session_state["pet_id"] = _generate_pet_id()
            pet_id = st.text_input("Pet ID", value=st.session_state["pet_id"], disabled=True)
            date_of_birth = st.date_input(
                "Date of birth",
                value=datetime.date(1990, 1, 1),
                max_value=datetime.date.today(),
            )
            st.subheader("Contact Information")
            pet_owner_phone = st.text_input("Pet Owner Phone number", placeholder="+1 555-123-4567")
            pet_owner_email = st.text_input("Pet Owner Email address", placeholder="jane.doe@example.com")
            address = st.text_area(
                "Home address", placeholder="123 Main St, City, State"
            )

            st.subheader("Visit Information")
            visit_date = st.date_input(
                "Visit date",
                value=datetime.date.today(),
                min_value=datetime.date(2000, 1, 1),
                max_value=datetime.date.today(),
            )
            department = st.selectbox(
                "Department",
                [
                    "General Medicine",
                    "Pediatrics",
                    "Cardiology",
                    "Orthopedics",
                    "Dermatology",
                    "Neurology",
                    "Emergency",
                ],
            )
            symptoms = st.text_area("Symptoms / Reason for visit")
            allergies = st.text_area("Known allergies (if any)")

            additional_notes = st.text_area("Additional notes")

            submitted = st.form_submit_button("Submit intake form")

        if submitted:
            missing_fields = []
            if not full_name.strip():
                missing_fields.append("Full name")
            if date_of_birth is None:
                missing_fields.append("Date of birth")
            if missing_fields:
                st.error(
                    "Please complete the required fields: "
                    + ", ".join(missing_fields)
                    + "."
                )
                return

            payload = {
                "full_name": full_name,
                "Pet_id": pet_id,
                "date_of_birth": date_of_birth,
                "phone": pet_owner_phone,
                "email": pet_owner_email,
                "address": address,
                "visit_date": visit_date,
                "department": department,
                "symptoms": symptoms,
                "allergies": allergies,
                "additional_notes": additional_notes,
            }

            try:
                insert_Pet_intake(payload)
            except Exception as exc:  # noqa: BLE001
                st.error(f"Submission failed: {exc}")
                return

            st.success("Pet intake form submitted to Lakebase.")
            st.write(payload)
            st.session_state["pet_id"] = _generate_pet_id()

    with records_tab:
        st.subheader("Pet Records")
        try:
            records = fetch_Pet_records()
        except Exception as exc:  # noqa: BLE001
            st.error(f"Unable to load records: {exc}")
            return

        if records:
            st.dataframe(records, use_container_width=True)
        else:
            st.info("No Pet records found yet.")



main()
