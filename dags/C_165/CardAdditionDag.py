from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow import XComArg
from airflow.hooks.base import BaseHook

from datetime import datetime
import re
from math import ceil
import typing
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, List, Type
from MySQLdb.cursors import DictCursor
from datetime import datetime, timedelta
import json

from lib.wekan.controllers.users import get_users
from lib.wekan.controllers.cards import (
    create_card,
    edit_card_data,
    edit_card_custom_field,
)
from lib.wekan.controllers.boards import get_board_custom_fields
from lib.wekan.types.boards import WekanConfiguration
from C_165.ticket_reasons import TicketReason, TicketReasonCategories, TICKET_REASONS

 
conn = BaseHook.get_connection("erta_wekan_robot")

hostname = conn.host
username = conn.login
password = conn.password


@dataclass
class ConnectionInfo:
    conn_id: str
    hook: Type[BaseHook]
    database: Optional[str] = None

CONNECTIONS = {
    "OPERATIONS_LOGGER": ConnectionInfo(conn_id="prd-az1-ops3-airflowconnection", hook=PostgresHook, database="sourceoftruth"),  # operations_logger
    "FORM_OPERATIONS": ConnectionInfo(conn_id="formoperations_prd_az1_opssql_database_windows_net", hook=MsSqlHook),  # formoperations
}


class dataCache(Enum):
    custom_fields = 1
    users = 2


@dataclass
class WekanInfo:
    board_id: str
    list_id: str
    swimlane_id: str
    author_id: str = "KwMkTND63bbwYCqew"
    member_ids: Optional[list[str]] = field(default_factory=list)


WEKAN_INFO = {
    TicketReasonCategories.CAT_1: WekanInfo(board_id='EoDvXoJDqRLQLzsDP', list_id='AaZCmJcnmHmfdKTPD', swimlane_id='ZhzN56MzDjQFzASFx'),
    TicketReasonCategories.CAT_2: WekanInfo(board_id='4wsbMowhKhHmeSGhi', list_id='9Lca5gLBPFuZJFwZ5', swimlane_id='MxtWxrBMHRwCjGDD4'),
    TicketReasonCategories.CAT_3: WekanInfo(board_id='wmC23eXvm8yT68Pfw', list_id='L9vq2gHE4biL9d8kW', swimlane_id='5HPLKr7XJYQsCeQ9K'),
    TicketReasonCategories.CAT_4: WekanInfo(board_id='wmC23eXvm8yT68Pfw', list_id='L9vq2gHE4biL9d8kW', swimlane_id='5HPLKr7XJYQsCeQ9K'),
    TicketReasonCategories.CAT_5: WekanInfo(board_id='wmC23eXvm8yT68Pfw', list_id='L9vq2gHE4biL9d8kW', swimlane_id='5HPLKr7XJYQsCeQ9K'),
    TicketReasonCategories.CAT_6: WekanInfo(board_id='zkBvpNGba4opyCrtM', list_id='Hru5Gy8P3kWsxevDa', swimlane_id='MHugnzB3ezMkc7yvK'),
    TicketReasonCategories.CAT_7: WekanInfo(board_id='zkBvpNGba4opyCrtM', list_id='hCdyxk2wL6p8JDEjT', swimlane_id='MHugnzB3ezMkc7yvK'),
    TicketReasonCategories.CAT_8: WekanInfo(board_id='zkBvpNGba4opyCrtM', list_id='Nq5o3nHHCoYfaRjEE', swimlane_id='MHugnzB3ezMkc7yvK'),
    TicketReasonCategories.CAT_9: WekanInfo(board_id='zkBvpNGba4opyCrtM', list_id='Nq5o3nHHCoYfaRjEE', swimlane_id='MHugnzB3ezMkc7yvK'),
    TicketReasonCategories.CAT_10: WekanInfo(board_id='EYhgN5mAsvyFhkxsY', list_id='96eCtn8uXu6Kbbc8c', swimlane_id='qSAF8gJw8Xwapvqb4'),
    TicketReasonCategories.CAT_11: WekanInfo(board_id='hC87vvkuMDXwJAxDp', list_id='7jCBn4nsrvNboJ9Zt', swimlane_id='zYXy8PY9xkvq8qYr5'),
    TicketReasonCategories.CAT_12: WekanInfo(board_id='sGyzAxaDDni7wbNF7', list_id='endRNYGKthW5AjPkF', swimlane_id='ypoPDmZbfFKFkSv7d'),
    TicketReasonCategories.CAT_13: WekanInfo(board_id='iPZ5T3uRQoRqqXrM3', list_id='PKzpTmE8StJnkSeeA', swimlane_id='3oJWqY8PwQL5E4ywX'),
    TicketReasonCategories.CAT_14: WekanInfo(board_id='iPZ5T3uRQoRqqXrM3', list_id='PKzpTmE8StJnkSeeA', swimlane_id='3oJWqY8PwQL5E4ywX')
}


ALTERNATE_DESCRIPTION = {
    TicketReasonCategories.CAT_7: "Developer Estimate Dependencies (2 other card types): Per L-139, Make sure data is available in an extract and verify Extract card associated with the Reference Database for this measure is in Completed state and review extracted data prior providing an estimate",
    TicketReasonCategories.CAT_11: "``` {description} ```",
}


@dataclass
class CustomPriority:
    id: str
    impact: int
    field_id: str
    matrix_field_id: str
    color: str


@dataclass
class Ticket:
    id: str
    escalation_option: str
    ticket_reason: TicketReason
    on_behalf_of: str
    client_legal_name: str
    description: str
    requestor_info: str
    priority: int
    wekan_info: WekanInfo
    card_title: str
    subscribed: bool
    project_ids: list[str]
    remove_html: bool
    raw_row: dict
    custom_priority: Optional[CustomPriority] = None


class ReturningPostgresOperator(PostgresOperator):
    def __init__(self, *args, database=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.database = database

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = PostgresHook(postgres_conn_id=self.conn_id, schema=self.database)
        return hook.get_records(self.sql, parameters=self.parameters)

default_args = {
    "owner": "airflow",
}
with DAG(
    "card_addition_dag",
    default_args=default_args,
    schedule=None,
    # schedule_interval='@monthly',
    # start_date=datetime(2025, 2, 1),
    tags=["wekan", "card-addition", "C-165", "Canary"],
) as dag:

    trigger_check = ReturningPostgresOperator(
        task_id="trigger_check",
        postgres_conn_id=CONNECTIONS["OPERATIONS_LOGGER"].conn_id,
        database=CONNECTIONS["OPERATIONS_LOGGER"].database,
        sql=f"""
        SELECT COUNT(*)
        FROM public.restart_trigger rt
        INNER JOIN public.schedule_jobs sj
            ON rt.trigger_name = sj.server
        WHERE rt.switch = 0
            AND sj.script_name = 'Internal Ticketing';
        """,
        dag=dag,
    )

    overlap_check = ReturningPostgresOperator(
        task_id="overlap_check",
        postgres_conn_id=CONNECTIONS["OPERATIONS_LOGGER"].conn_id,
        sql=f"""
        SELECT COUNT(*)
        FROM public.job_triggers
        WHERE associated_table = 'card_addition_erta_com_konza_support_curl'
            AND trigger_status = 0;
        """,
        dag=dag,
    )

    update_job_triggers = ReturningPostgresOperator(
        task_id="update_job_triggers",
        postgres_conn_id=CONNECTIONS["OPERATIONS_LOGGER"].conn_id,
        database=CONNECTIONS["OPERATIONS_LOGGER"].database,
        sql=f"""
        UPDATE public.job_triggers
        SET trigger_status = 1
        WHERE associated_table = 'card_addition_erta_com_konza_support_curl';
        """,
        dag=dag,
    )

    @task
    def trigger_control_task(trigger_check_in, overlap_check_in):
        if not trigger_check_in[0][0] or not overlap_check_in[0][0]:
            raise AirflowSkipException

    def _stringify_field(field: Any) -> str:
        return str(field).strip() if field else ""

    def _enforce_min_chars(input: str, min_chars: int) -> str:
        return input if len(input) >= min_chars else ""

    def _generate_support_ticket_query_str() -> str:
        proj_id_cols = ["project_ids", "ticket_reference_other_system"]
        project_codes = ["L-", "S-", "C-"]
        conditions = " OR ".join(
            [f"{col} LIKE '{code}%'" for col in proj_id_cols for code in project_codes]
        )
        sql = f"""
        SELECT data_extract_description,escalation_option,project_ids,L_120__authorized_identifier,L_120__authorized_identifier_type,L_120__hl7v2_client_delivery_paused,L_120__authorization_party,L_120__id,L_102__authorized_identifier,L_120__hl7v2_client_name,L_68__id,L_68__authorization_reference,L_68__authorized_identifier_oid,L_68__authorized_filter_file_beginning_pattern,L_68__authorizing_party,L_68__delivery_paused,L_68__participant_client_name,L_68__source_base_path,L_68__destination_path_override,L_68__client_folder_name,audit_type,L_69__id, L_69__source_file_zipped, L_102__authorized_identifier_type,L_102__hl7v2_client_delivery_paused,L_102__authorization_party,L_102__id,L_102__authorized_identifier,L_102__hl7v2_client_name,L_87__client_folder_name,L_87__destination_path_override,L_87__source_base_path,L_87__participant_client_name,L_87__authorization_reference,L_87__delivery_paused,L_87__authorizing_party,L_87__authorized_filter_file_beginning_pattern,L_87__authorized_identifier_oid,L_87__id,id,behalf_of_email,client_legal_name,ticket_reason,ticket_reference_other_system,requestor_info, ehx__ID ,ehx__ssi_payor_client_name ,ehx__authorized_identifier ,ehx__authorization_party ,ehx__dh_fusion_delivery_paused ,ehx__authorized_identifer_type ,ehx__konza_client_destination_delivery_paused ,ehx__dh_raw_ehx_ccd_delivery_location ,L_10__id ,L_10__hl7v2_client_name ,L_10__authorized_identifier ,L_10__authorization_party ,L_10__authorized_identifier_type ,L_10__crawler_id_last_loaded, L_10__hl7v2_client_delivery_paused ,L_69__emr_client_name ,L_69__authorized_identifier ,L_69__authorization_party ,L_69__emr_client_delivery_paused ,L_69__authorized_identifier_type ,L_69__authorization_reference ,L_69__participant_client_name ,ticket_reason_extended ,attachment,ehx__ssi_prefix,ehx__auto_submit_panel_to_corepoint 
        FROM konza_support
        WHERE added_card_reference is null
        AND {conditions}
        ORDER by id desc;
        """
        return sql

    def _get_ticket_reason(ticket_reason_text: str) -> TicketReason:
        for reason in TICKET_REASONS:
            if ticket_reason_text == reason.text:
                return reason
        raise ValueError(
            f'The supplied support ticket reason "{ticket_reason_text}" is not found within the defined critera.'
        )

    def _get_project_priority(project_ids: List[str], all_projects: dict) -> int:
        for project in all_projects:
            if project["ProjectId"] in project_ids:
                return project["Priority"]
        return 0

    def _get_description(ticket_reason_enum: TicketReasonCategories, description: str):
        if ticket_reason_enum not in ALTERNATE_DESCRIPTION:
            return description
        alt_str = ALTERNATE_DESCRIPTION[ticket_reason_enum]
        if "{description}" in alt_str:
            alt_str = alt_str.replace("{description}", description)
        return alt_str

    def _get_card_title(
        id: str,
        ticket_reason: str,
        client_or_particiant_name: str,
        client_legal_name: str,
        external_ticket_ref: str,
        description: str,
        requestor_info: str,
        on_behalf_of: str,
    ) -> str:
        card_title = f"GID:SUP-{id} {ticket_reason} Client/Participant Name: {client_or_particiant_name}"
        card_title = (
            card_title + " - " + client_legal_name
            if len(client_legal_name) > 1
            else card_title
        )
        card_title = (
            card_title + " - " + external_ticket_ref
            if len(external_ticket_ref) > 1
            else card_title
        )
        card_title = (
            card_title + " - " + description.strip()[:100] + "..."
            if len(description) > 1
            else card_title
        )
        card_title = card_title + " - Requestor: " + requestor_info[:50].strip()
        card_title = (
            card_title + " - On Behalf of: " + on_behalf_of.strip()
            if len(on_behalf_of) > 1
            else card_title
        )
        return card_title

    def _generate_attachment_appendix(attachment: str) -> str:
        if len(attachment) > 1:
            return (
                " | Link: https://konzainc-my.sharepoint.com/personal/form_automation_konza_org/Documents/Apps/Microsoft%20Forms/KONZA%20Support/Question/"
                + str(attachment)
                .split('name":"')[1]
                .split('","link')[0]
                .replace(" ", "%20")
                + " | <br><br><br><br><br> Full Attachment Reference: "
                + str(attachment)
            )
        return ""

    def _extract_results_into_support_tickets(
        results: dict, all_proj_results: dict
    ) -> List[Ticket]:
        support_tickets = []
        for row in results:
            ticket_reason_text = _stringify_field(row["ticket_reason"])
            ticket_reason = _get_ticket_reason(ticket_reason_text)
            project_id_other = _stringify_field(row["ticket_reference_other_system"])
            project_ids = [row["project_ids"], project_id_other]
            description = _get_description(
                ticket_reason.category,
                f"{_stringify_field(row['data_extract_description'])} Description: {_stringify_field(row['ticket_reason_extended'])}",
            )
            id = row["id"]
            client_legal_name = _stringify_field(row["client_legal_name"])
            external_ticket_ref = _enforce_min_chars(project_id_other, 2)
            requestor_info = _stringify_field(row["requestor_info"])
            on_behalf_of = _stringify_field(row["behalf_of_email"])
            client_or_participant_name = "|".join(
                [
                    _stringify_field(row[x])
                    for x in [
                        "ehx__ssi_payor_client_name",
                        "L_10__hl7v2_client_name",
                        "L_69__emr_client_name",
                        "L_69__participant_client_name",
                        "L_68__participant_client_name",
                        "L_68__client_folder_name",
                        "client_legal_name",
                        "L_87__participant_client_name",
                        "L_87__client_folder_name",
                        "L_102__hl7v2_client_name",
                    ]
                ]
            )

            support_tickets.append(
                Ticket(
                    id=id,
                    escalation_option=_stringify_field(
                        row["escalation_option"]
                        if "-" in row["escalation_option"]
                        else "null"
                    ),
                    ticket_reason=ticket_reason,
                    on_behalf_of=on_behalf_of,
                    client_legal_name=client_legal_name,
                    description=description
                    + _generate_attachment_appendix(
                        _stringify_field(row["attachment"])
                    ),
                    requestor_info=requestor_info,
                    priority=_get_project_priority(project_ids, all_proj_results),
                    wekan_info=WEKAN_INFO[ticket_reason.category],
                    subscribed=requestor_info in ticket_reason.subscribers,
                    card_title=_get_card_title(
                        id,
                        ticket_reason.text,
                        client_or_participant_name,
                        client_legal_name,
                        external_ticket_ref,
                        description,
                        requestor_info,
                        on_behalf_of,
                    ),
                    project_ids=project_ids,
                    remove_html=(
                        True if ticket_reason.id in [41, 42] else False
                    ),  # Try to avoid adding more conditionals based on task ID to this function - it will get hard to follow quickly.
                    raw_row=row,
                )
            )
        return support_tickets

    def _execute_query(sql: str, conn_info: ConnectionInfo, return_dict: bool = True) -> dict:
        hook = conn_info.hook(**{conn_info.hook.conn_name_attr: conn_info.conn_id, "schema": conn_info.database})
        with hook.get_conn() as conn:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute(sql)
                if return_dict:
                    return cursor.fetchall()

    @task
    def process_support_tickets_task(configuration: XComArg):
        support_results = _execute_query(
            _generate_support_ticket_query_str(), CONNECTIONS["FORM_OPERATIONS"]
        )
        all_projects_results = _execute_query(
            "SELECT ProjectId, Priority from all_projects;",
            CONNECTIONS["FORM_OPERATIONS"],
        )
        support_tickets = _extract_results_into_support_tickets(
            support_results, all_projects_results
        )

        data_cache = {dataCache.custom_fields: {}, dataCache.users: {}}
        parsed_configuration = typing.cast(WekanConfiguration, configuration)
        for support_ticket in support_tickets:
            _process_ticket(support_ticket, data_cache, parsed_configuration)

    def _process_ticket(
        ticket: Ticket,
        data_cache: dict[dataCache, dict],
        parsed_configuration: WekanConfiguration,
    ):
        _set_ticket_custom_priority(
            ticket, data_cache[dataCache.custom_fields], parsed_configuration
        )
        do_create_wekan_card = (
            _update_tables(ticket)
            if (ticket.subscribed or ticket.ticket_reason.flag_unsubscribed_access)
            else True
        )
        if do_create_wekan_card:
            _create_wekan_card(ticket, data_cache, parsed_configuration)
        _execute_query(
            """update public.job_triggers set trigger_status = '0' where associated_table = 'card_addition_erta_com_konza_support_curl';""",
            CONNECTIONS["OPERATIONS_LOGGER"],
            return_dict=False,
        )

    def _create_wekan_card(
        ticket: Ticket,
        data_cache: dict[dataCache, dict],
        parsed_configuration: WekanConfiguration,
    ):
        print(f"Creating wekan card: {ticket.card_title}")
        wekan_users = _get_and_cache_users_from_board(
            data_cache[dataCache.users], parsed_configuration
        )
        if ticket.remove_html:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(ticket.description, "html5lib")
            ticket.description = soup.get_text(separator=r"\n\n\n")
        response = _add_card_to_board(ticket, wekan_users, parsed_configuration)
        new_card_id = response["_id"]

        form_sql = (
            "update konza_support set added_card_reference = '"
            + new_card_id
            + "' , project_ids = '"
            + ",".join(ticket.project_ids).replace("'", "")
            + "', prioritization = '"
            + str(ticket.priority)
            + "'  where id = '"
            + str(ticket.id)
            + "'"
        )
        _execute_query(form_sql, CONNECTIONS["FORM_OPERATIONS"], return_dict=False)

        # sql = f"""
        #    INSERT INTO public.global_ticket_crosswalk (global_id, ticketing_system_id, ticketing_system)
        #    VALUES (nextval('global_ticket_crosswalk_seq'),
        #            '{new_card_id}',
        #            '{ticket.wekan_info.board_id}')
        # """

        # Removed 'nextval' for test, as not present in mariadb
        sql = f"""
            INSERT INTO public.global_ticket_crosswalk (ticketing_system_id, ticketing_system)
            VALUES ('{new_card_id}', 
                    '{ticket.wekan_info.board_id}')
        """
        # check is using the right connection - not clear from script.
        _execute_query(sql, CONNECTIONS["OPERATIONS_LOGGER"], return_dict=False)

        _edit_custom_fields(new_card_id, ticket, parsed_configuration)
        _edit_card_color(new_card_id, ticket, parsed_configuration)

        if ticket.ticket_reason.id == 36:  # 'Internal Audit'
            ticket.custom_priority.color = "crimson"
            due_at = str(datetime.now() + timedelta(hours=6))
            _edit_internal_audit_card(
                new_card_id,
                ticket,
                due_at,
                new_list_id="489T6a7mkEwJgNva8",
                parsed_configuration=parsed_configuration,
            )

    def _update_tables(ticket: TicketReason) -> bool:
        do_create_wekan_card = True
        new_card_id = "Skipped as automated"
        row = ticket.raw_row
        sql = None
        match ticket.ticket_reason.id:
            case 8:
                l102authorizedIdentifier = row["L_102__authorized_identifier"]
                # Unsure what object type this would now be, guessing list
                if isinstance(row["L_102__authorized_identifier"], list):
                    l102authorizedIdentifier = ""
                sql = (
                    """INSERT INTO public.raw_hl7v2_feed__l_102 (authorized_identifier,
                                    hl7v2_client_name,
                                    id,
                                    authorization_party,
                                    authorized_identifier_type,
                                    hl7v2_client_delivery_paused)
                    VALUES ('"""
                    + l102authorizedIdentifier
                    + """',
                        '"""
                    + row["L_102__hl7v2_client_name"]
                    + """',
                        '"""
                    + row["L_102__id"]
                    + """',
                        '"""
                    + row["L_102__authorization_party"]
                    + """',
                            '"""
                    + row["L_102__authorized_identifier_type"]
                    + """',
                            '"""
                    + row["L_102__hl7v2_client_delivery_paused"]
                    + """')

                            ON CONFLICT (id)
                            DO UPDATE SET 
                                    authorized_identifier ='"""
                    + l102authorizedIdentifier
                    + """',
                                    hl7v2_client_name = '"""
                    + row["L_102__hl7v2_client_name"]
                    + """',
                                    authorization_party = '"""
                    + row["L_102__authorization_party"]
                    + """',
                                    authorized_identifier_type = '"""
                    + row["L_102__authorized_identifier_type"]
                    + """',
                                    hl7v2_client_delivery_paused = '"""
                    + row["L_102__hl7v2_client_delivery_paused"]
                    + """'
                            ;"""
                )
                do_create_wekan_card = False

            case 9:
                sql = (
                    """INSERT INTO public.raw_hl7v2_feed__l_120 (authorized_identifier,
                                    hl7v2_client_name,
                                    id,
                                    authorization_party,
                                    authorized_identifier_type,
                                    hl7v2_client_delivery_paused)
                    VALUES ('"""
                    + row["L_120__authorized_identifier"]
                    + """',
                        '"""
                    + row["L_120__hl7v2_client_name"]
                    + """',
                        '"""
                    + row["L_120__id"]
                    + """',
                        '"""
                    + row["L_120__authorization_party"]
                    + """',
                            '"""
                    + row["L_120__authorized_identifier_type"]
                    + """',
                            '"""
                    + row["L_120__hl7v2_client_delivery_paused"]
                    + """')

                            ON CONFLICT (id)
                            DO UPDATE SET 
                                    authorized_identifier ='"""
                    + row["L_120__authorized_identifier"]
                    + """',
                                    hl7v2_client_name = '"""
                    + row["L_120__hl7v2_client_name"]
                    + """',
                                    authorization_party = '"""
                    + row["L_120__authorization_party"]
                    + """',
                                    authorized_identifier_type = '"""
                    + row["L_120__authorized_identifier_type"]
                    + """',
                                    hl7v2_client_delivery_paused = '"""
                    + row["L_120__hl7v2_client_delivery_paused"]
                    + """'
                            ;"""
                )
                do_create_wekan_card = False

            case 16:
                sql = (
                    """INSERT INTO public.archive_delivery__l_68 (id,
                                                        authorized_identifier_oid,
                                                        authorized_filter_file_beginning_pattern,
                                                        authorizing_party,
                                                        delivery_paused,
                                                        authorization_reference,
                                                        participant_client_name,
                                                        source_base_path,
                                                        destination_path_override,
                                                        client_folder_name
                                                        )
                                        VALUES ('"""
                    + row["L_68__id"]
                    + """',
                                            '"""
                    + row["L_68__authorized_identifier_oid"]
                    + """',
                                            '"""
                    + row["L_68__authorized_filter_file_beginning_pattern"]
                    + """',
                                            '"""
                    + row["L_68__authorizing_party"]
                    + """',
                                                '"""
                    + row["L_68__delivery_paused"]
                    + """',
                                                '"""
                    + row["L_68__authorization_reference"]
                    + """',
                                                '"""
                    + row["L_68__participant_client_name"]
                    + """',
                                                '"""
                    + row["L_68__source_base_path"]
                    + """',
                                                '"""
                    + row["L_68__destination_path_override"]
                    + """',
                                                '"""
                    + row["L_68__client_folder_name"]
                    + """')

                                                ON CONFLICT (id)
                                                DO UPDATE SET 
                                                        authorized_identifier_oid ='"""
                    + row["L_68__authorized_identifier_oid"]
                    + """',
                                                        authorized_filter_file_beginning_pattern ='"""
                    + row["L_68__authorized_filter_file_beginning_pattern"]
                    + """',
                                                        authorizing_party ='"""
                    + row["L_68__authorizing_party"]
                    + """',
                                                        delivery_paused ='"""
                    + row["L_68__delivery_paused"]
                    + """',
                                                        authorization_reference ='"""
                    + row["L_68__authorization_reference"]
                    + """',
                                                        participant_client_name ='"""
                    + row["L_68__participant_client_name"]
                    + """',
                                                        source_base_path ='"""
                    + row["L_68__source_base_path"]
                    + """',
                                                        destination_path_override ='"""
                    + row["L_68__destination_path_override"]
                    + """',
                                                        client_folder_name = '"""
                    + row["L_68__client_folder_name"]
                    + """'
                                                ;"""
                )
                do_create_wekan_card = False

            case 11:
                if (
                    len(row["L_69__emr_client_name"]) > 2
                    and len(row["L_69__authorized_identifier"]) > 2
                    and len(row["L_69__authorization_party"]) > 2
                    and len(row["L_69__authorized_identifier_type"]) > 2
                    and len(row["L_69__authorization_reference"]) > 2
                    and len(row["L_69__participant_client_name"]) > 2
                ):
                    sql = (
                        """INSERT INTO public.cda_konza_sftp_retrieval__l_69 (id,
                                        emr_client_name,
                                        authorized_identifier,
                                        authorization_party,
                                        emr_client_delivery_paused,
                                        authorized_identifier_type,
                                        authorization_reference,
                                        participant_client_name,
                                        source_files_zipped)
                        VALUES ('"""
                        + row["L_69__id"]
                        + """',
                            '"""
                        + row["L_69__emr_client_name"]
                        + """',
                            '"""
                        + row["L_69__authorized_identifier"]
                        + """',
                            '"""
                        + row["L_69__authorization_party"]
                        + """',
                            '"""
                        + row["L_69__emr_client_delivery_paused"]
                        + """',
                            '"""
                        + row["L_69__authorized_identifier_type"]
                        + """',
                                '"""
                        + row["L_69__authorization_reference"]
                        + """',
                                '"""
                        + row["L_69__participant_client_name"]
                        + """',
                                '"""
                        + row["L_69__source_file_zipped"]
                        + """')

                                ON CONFLICT (id)
                                DO UPDATE SET 
                                        emr_client_name ='"""
                        + row["L_69__emr_client_name"]
                        + """',
                                        authorized_identifier ='"""
                        + row["L_69__authorized_identifier"]
                        + """',
                                        authorization_party ='"""
                        + row["L_69__authorization_party"]
                        + """',
                                        emr_client_delivery_paused ='"""
                        + row["L_69__emr_client_delivery_paused"]
                        + """',
                                        authorized_identifier_type ='"""
                        + row["L_69__authorized_identifier_type"]
                        + """',
                                        authorization_reference = '"""
                        + row["L_69__authorization_reference"]
                        + """',
                                        participant_client_name = '"""
                        + row["L_69__participant_client_name"]
                        + """',
                                        source_files_zipped = '"""
                        + row["L_69__source_file_zipped"]
                        + """'
                                ;"""
                    )

                if (
                    len(row["L_69__emr_client_name"]) < 2
                    and len(row["L_69__authorized_identifier"]) < 2
                    and len(row["L_69__authorization_party"]) < 2
                    and len(row["L_69__authorized_identifier_type"]) < 2
                    and len(row["L_69__authorization_reference"]) < 2
                    and len(row["L_69__participant_client_name"]) < 2
                ):
                    sql = (
                        """INSERT INTO public.cda_konza_sftp_retrieval__l_69 (id,
                                        emr_client_delivery_paused,
                                        source_files_zipped)
                        VALUES ('"""
                        + row["L_69__id"]
                        + """',
                            '"""
                        + row["L_69__emr_client_delivery_paused"]
                        + """',
                                '"""
                        + row["L_69__source_file_zipped"]
                        + """')
                                ON CONFLICT (id)
                                DO UPDATE SET 
                                        emr_client_delivery_paused ='"""
                        + row["L_69__emr_client_delivery_paused"]
                        + """',
                                        source_files_zipped = '"""
                        + row["L_69__source_file_zipped"]
                        + """'
                                ;"""
                    )
                if "INSERT INTO public.cda_konza_sftp_retrieval__l_69" in sql:
                    do_create_wekan_card = False
                if "INSERT INTO public.cda_konza_sftp_retrieval__l_69" not in sql:
                    sql = None  # Don't run operations_logger query.
                    ticket.card_title = (
                        "Automated Process Missing by User Input " + ticket.card_title
                    )
                    print("Not skipping Card Creation - IF conditions above not met")
                    ## KONZA - Help Desk
                    ticket.wekan_info.author_id = (
                        "KJkuCf5wXdRjYHrcb"  # Bot is default #KJkuCf5wXdRjYHrcb
                    )
                    ticket.wekan_info.board_id = "ZtFXLZ2pjP6f3MzKW"
                    ticket.wekan_info.list_id = "Zt9rtpPjrdHnHzrbT"
                    ticket.wekan_info.swimlane_id = "6vQwEdCJW55x4pFBd"
                    ticket.wekan_info.member_ids.append("mZcXKPorDi5KYwjRT")
            case 10:
                sql = (
                    """INSERT INTO public.raw_hl7v2_feed__l_10 (authorized_identifier,
                                    hl7v2_client_name,
                                    id,
                                    authorization_party,
                                    authorized_identifier_type,
                                    hl7v2_client_delivery_paused)
                    VALUES ('"""
                    + row["L_10__authorized_identifier"]
                    + """',
                        '"""
                    + row["L_10__hl7v2_client_name"]
                    + """',
                        '"""
                    + row["L_10__id"]
                    + """',
                        '"""
                    + row["L_10__authorization_party"]
                    + """',
                            '"""
                    + row["L_10__authorized_identifier_type"]
                    + """',
                            '"""
                    + row["L_10__hl7v2_client_delivery_paused"]
                    + """')

                            ON CONFLICT (id)
                            DO UPDATE SET 
                                    authorized_identifier ='"""
                    + row["L_10__authorized_identifier"]
                    + """',
                                    hl7v2_client_name = '"""
                    + row["L_10__hl7v2_client_name"]
                    + """',
                                    authorization_party = '"""
                    + row["L_10__authorization_party"]
                    + """',
                                    authorized_identifier_type = '"""
                    + row["L_10__authorized_identifier_type"]
                    + """',
                                    hl7v2_client_delivery_paused = '"""
                    + row["L_10__hl7v2_client_delivery_paused"]
                    + """'
                            ;"""
                )
                do_create_wekan_card = False

            case 63:
                do_create_wekan_card = False
                if not ticket.subscribe:
                    new_card_id = "Unauthorized Request"
                else:
                    sql = (
                        """INSERT INTO public.konza_ssi_ehealth_exchange_distribution (id,
                            ssi_payor_client_name,
                            authorized_identifier,
                            authorization_party,
                            dh_fusion_delivery_paused,
                            authorized_identifier_type,
                            konza_client_destination_delivery_paused,
                            ssi_prefix,
                            auto_submit_panel_to_corepoint,
                            dh_raw_ehx_ccd_delivery_location)
                                VALUES ('"""
                        + row["ehx__ID"]
                        + """',
                                    '"""
                        + row["ehx__ssi_payor_client_name"]
                        + """',
                                    '"""
                        + row["ehx__authorized_identifier"]
                        + """',
                                    '"""
                        + row["ehx__authorization_party"]
                        + """',
                                    '"""
                        + row["ehx__dh_fusion_delivery_paused"]
                        + """',
                                        '"""
                        + row["ehx__authorized_identifer_type"]
                        + """',
                                        '"""
                        + row["ehx__konza_client_destination_delivery_paused"]
                        + """',
                                        '"""
                        + row["ehx__ssi_prefix"]
                        + """',
                                        '"""
                        + row["ehx__auto_submit_panel_to_corepoint"]
                        + """',
                                        '"""
                        + row["ehx__dh_raw_ehx_ccd_delivery_location"]
                        + """')

                                        ON CONFLICT (id)
                                        DO UPDATE SET 
                                                ssi_payor_client_name ='"""
                        + row["ehx__ssi_payor_client_name"]
                        + """',
                                                authorized_identifier = '"""
                        + row["ehx__authorized_identifier"]
                        + """',
                                                authorization_party = '"""
                        + row["ehx__authorization_party"]
                        + """',
                                                dh_fusion_delivery_paused = '"""
                        + row["ehx__dh_fusion_delivery_paused"]
                        + """',
                                                authorized_identifier_type = '"""
                        + row["ehx__authorized_identifer_type"]
                        + """',
                                                konza_client_destination_delivery_paused = '"""
                        + row["ehx__konza_client_destination_delivery_paused"]
                        + """',
                                                ssi_prefix = '"""
                        + row["ehx__ssi_prefix"]
                        + """',
                                                auto_submit_panel_to_corepoint = '"""
                        + row["ehx__auto_submit_panel_to_corepoint"]
                        + """',
                                                dh_raw_ehx_ccd_delivery_location = '"""
                        + row["ehx__dh_raw_ehx_ccd_delivery_location"]
                        + """'
                                        ;"""
                    )

            case 64:
                sql = (
                    """INSERT INTO public.bcbsks_verinovum__l_87 (id,
                                    authorized_identifier_oid,
                                    authorized_filter_file_beginning_pattern,
                                    authorizing_party,
                                    delivery_paused,
                                    authorization_reference,
                                    participant_client_name,
                                    source_base_path,
                                    destination_path_override,
                                    client_folder_name)
                    VALUES ('"""
                    + row["L_87__id"]
                    + """',
                        '"""
                    + row["L_87__authorized_identifier_oid"]
                    + """',
                        '"""
                    + row["L_87__authorized_filter_file_beginning_pattern"]
                    + """',
                        '"""
                    + row["L_87__authorizing_party"]
                    + """',
                            '"""
                    + row["L_87__delivery_paused"]
                    + """',
                            '"""
                    + row["L_87__authorization_reference"]
                    + """',
                            '"""
                    + row["L_87__participant_client_name"]
                    + """',
                            '"""
                    + row["L_87__source_base_path"]
                    + """',
                            '"""
                    + row["L_87__destination_path_override"]
                    + """',
                            '"""
                    + row["L_87__client_folder_name"]
                    + """')

                            ON CONFLICT (id)
                            DO UPDATE SET 
                                    authorized_identifier_oid ='"""
                    + row["L_87__authorized_identifier_oid"]
                    + """',
                                    authorized_filter_file_beginning_pattern = '"""
                    + row["L_87__authorized_filter_file_beginning_pattern"]
                    + """',
                                    authorizing_party = '"""
                    + row["L_87__authorizing_party"]
                    + """',
                                    delivery_paused = '"""
                    + row["L_87__delivery_paused"]
                    + """',
                                    authorization_reference = '"""
                    + row["L_87__authorization_reference"]
                    + """',
                                    participant_client_name = '"""
                    + row["L_87__participant_client_name"]
                    + """',
                                    source_base_path = '"""
                    + row["L_87__source_base_path"]
                    + """',
                                    destination_path_override = '"""
                    + row["L_87__destination_path_override"]
                    + """',
                                    client_folder_name = '"""
                    + row["L_87__client_folder_name"]
                    + """'
                            ;"""
                )
                do_create_wekan_card = False

            case 65, 66:
                sql = (
                    """INSERT INTO public.raw_hl7v2_feed__l_10 (authorized_identifier,
                                    hl7v2_client_name,
                                    id,
                                    authorization_party,
                                    authorized_identifier_type,
                                    hl7v2_client_delivery_paused)
                    VALUES ('"""
                    + row["L_10__authorized_identifier"]
                    + """',
                        '"""
                    + row["L_10__hl7v2_client_name"]
                    + """',
                        '"""
                    + row["L_10__id"]
                    + """',
                        '"""
                    + row["L_10__authorization_party"]
                    + """',
                            '"""
                    + row["L_10__authorized_identifier_type"]
                    + """',
                            '"""
                    + row["L_10__hl7v2_client_delivery_paused"]
                    + """')

                            ON CONFLICT (id)
                            DO UPDATE SET 
                                    authorized_identifier ='"""
                    + row["L_10__authorized_identifier"]
                    + """',
                                    hl7v2_client_name = '"""
                    + row["L_10__hl7v2_client_name"]
                    + """',
                                    authorization_party = '"""
                    + row["L_10__authorization_party"]
                    + """',
                                    authorized_identifier_type = '"""
                    + row["L_10__authorized_identifier_type"]
                    + """',
                                    hl7v2_client_delivery_paused = '"""
                    + row["L_10__hl7v2_client_delivery_paused"]
                    + """'
                            ;"""
                )
                do_create_wekan_card = False

        if sql:
            _execute_query(sql, CONNECTIONS["OPERATIONS_LOGGER"], return_dict=False)
            print(
                "Skipping Card addition to board as preauthorized - ticket reason: "
                + ticket.ticket_reason
            )

        if not do_create_wekan_card:
            form_sql = (
                "update konza_support set added_card_reference = '"
                + new_card_id
                + "' , project_ids = '"
                + ",".join(ticket.project_ids).replace("'", "")
                + "', prioritization = '"
                + ticket.priority
                + "'  where id = "
                + ticket.id
            )
            _execute_query(form_sql, CONNECTIONS["FORM_OPERATIONS"], return_dict=False)

        return do_create_wekan_card

    def _set_ticket_custom_priority(
        ticket: Ticket,
        custom_fields_cache: dict,
        parsed_configuration: WekanConfiguration,
    ) -> None:
        if ticket.escalation_option is "null":
            average_impact = -1
            risk_str = ""
        else:
            risk_str, first_in_rng, second_in_rng = _parse_priority_str(
                ticket.escalation_option
            )
            average_impact = ceil((first_in_rng + second_in_rng) / 2)
            # average_impact = ticket.escalation_option
            ### ASK: original script impact is set to equal escalation_option, is this a mistake?

        custom_fields = (
            custom_fields_cache[ticket.wekan_info.board_id]
            if ticket.wekan_info.board_id in custom_fields_cache
            else _get_and_cache_custom_fields_from_board(
                ticket.wekan_info.board_id, custom_fields_cache, parsed_configuration
            )
        )
        ticket.custom_priority = CustomPriority(
            id=risk_str,
            impact=average_impact,
            field_id=custom_fields["Priority"],
            matrix_field_id=custom_fields["Priority Matrix Number"],
            color=_get_custom_priority_color(risk_str),
        )

    def _get_custom_priority_color(priority_id: str) -> str:
        match priority_id:
            case "Critical":
                return "crimson"
            case "High":
                return "red"
            case "Medium":
                return "orange"
            case "Low":
                return "lime"
            case _:
                return ""

    ### ASK: What does the case with for if '["' in EscalationOption look like, will currently fail parsing
    def _parse_priority_str(priority_str) -> tuple[int, int]:
        match = re.search(r"(\S+) . (\d+)-(\d+)", priority_str)
        if match:
            return match.group(1), int(match.group(2)), int(match.group(3))
        raise ValueError(f"No range found in the input string: {priority_str}")

    def _get_and_cache_custom_fields_from_board(
        board_id, custom_fields_cache: dict, parsed_configuration: WekanConfiguration
    ) -> dict:
        response = get_board_custom_fields(
            parsed_configuration.get("hostname"),
            parsed_configuration.get("token"),
            board_id,
        )
        custom_fields = {}
        for field in response:
            custom_fields[field["name"]] = field["_id"]
        custom_fields_cache[board_id] = custom_fields
        return custom_fields

    def _get_and_cache_users_from_board(
        user_cache: dict, parsed_configuration: WekanConfiguration
    ) -> dict:
        if not user_cache:
            user_response = get_users(
                hostname=parsed_configuration.get("hostname"),
                token=parsed_configuration.get("token"),
            )
            for user in user_response:
                user_cache.update({user["username"]: user["_id"]})
        return user_cache

    def _add_card_to_board(
        ticket: Ticket, wekan_users: list, parsed_configuration: WekanConfiguration
    ) -> dict[str, str]:
        info = ticket.wekan_info
        _extract_member_ids_to_ticket(ticket, wekan_users)
        data = {
            "title": ticket.card_title,
            "description": ticket.description,
            "authorId": info.author_id,
            "swimlaneId": info.swimlane_id,
            "members": info.member_ids,
        }
        response = create_card(
            hostname=parsed_configuration.get("hostname"),
            token=parsed_configuration.get("token"),
            board_id=info.board_id,
            list_id=info.list_id,
            card_payload=data,
        )
        return response

    def _edit_card_color(
        card_id: str, ticket: Ticket, parsed_configuration: WekanConfiguration
    ):
        info = ticket.wekan_info
        data = {"color": ticket.custom_priority.color}
        response = edit_card_data(
            hostname=parsed_configuration.get("hostname"),
            token=parsed_configuration.get("token"),
            board_id=info.board_id,
            list_id=info.list_id,
            card_id=card_id,
            payload=data,
        )

    # ASK check the data values are correct
    def _edit_custom_fields(
        card_id: str, ticket: Ticket, parsed_configuration: WekanConfiguration
    ):
        info = ticket.wekan_info
        cust_p = ticket.custom_priority

        response = edit_card_custom_field(
            hostname=parsed_configuration.get("hostname"),
            token=parsed_configuration.get("token"),
            board_id=info.board_id,
            list_id=info.list_id,
            card_id=card_id,
            custom_field_id=cust_p.field_id,
            value=cust_p.id,
        )

        matrix_response = edit_card_custom_field(
            hostname=parsed_configuration.get("hostname"),
            token=parsed_configuration.get("token"),
            board_id=info.board_id,
            list_id=info.list_id,
            card_id=card_id,
            custom_field_id=cust_p.matrix_field_id,
            value=cust_p.impact,
        )

    def _edit_internal_audit_card(
        card_id: str,
        ticket: Ticket,
        due_at: str,
        new_list_id: str,
        parsed_configuration: WekanConfiguration,
    ):
        info = ticket.wekan_info
        data = {
            "title": ticket.card_title,
            "description": ticket.description,
            "color": ticket.custom_priority.color,
            "dueAt": due_at,
            "listId": new_list_id,
        }
        response = edit_card_data(
            hostname=parsed_configuration.get("hostname"),
            token=parsed_configuration.get("token"),
            board_id=info.board_id,
            list_id=info.list_id,
            card_id=card_id,
            payload=data,
        )
        ticket.list_id = new_list_id

    def _extract_member_ids_to_ticket(ticket: Ticket, wekan_users: dict):
        relevant_users = [
            _sanitize_user(ticket.requestor_info),
            _sanitize_user(ticket.on_behalf_of),
        ]
        for username, member_id in wekan_users.items():
            if _sanitize_user(username) in relevant_users:
                ticket.wekan_info.member_ids.append(member_id)

    def _sanitize_user(user: str) -> str:
        email_removed = user.split("@konza.org")[0]
        lower_case = email_removed.lower()
        return lower_case

    @task
    def login_user_task(
        hostname: str,
        username: str,
        password: str,
    ):
        """
        This function logs in the user to the wekan servers.
        """

        from lib.wekan.controllers.login import login

        response = login(hostname=hostname, username=username, password=password)
        error = response.get("error")

        if error:
            print(f"error: {error}")
            print(f"response: {response} {type(response)}")
            error_dict = {
                "status_code": error,
                "detail": {"response": json.dumps(response)},
            }
            raise AirflowException(error_dict)

        output = {**response, "hostname": hostname}
        return output
        
    configuration = login_user_task(
        hostname=hostname,
        username=username,
        password=password,
    )

    process_support_tickets = process_support_tickets_task(configuration)

    (
        trigger_control_task(trigger_check.output, overlap_check.output)
        >> configuration
        >> (
            update_job_triggers,
            process_support_tickets,
        )
    )
