from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

@dataclass
class TicketReason:
    id: int
    category: int
    text: str
    subscribers: Optional[list[str]] = field(default_factory=list)
    flag_unsubscribed_access: Optional[bool] = False


class TicketReasonCategories(Enum):
    CAT_1 = 1
    CAT_2 = 2
    CAT_3 = 3
    CAT_4 = 4
    CAT_5 = 5
    CAT_6 = 6
    CAT_7 = 7
    CAT_8 = 8
    CAT_9 = 9
    CAT_10 = 10
    CAT_11 = 11
    CAT_12 = 12
    CAT_13 = 13
    CAT_14 = 14
    NONE = 15


TICKET_REASONS = [
    TicketReason(
        id=1,
        category=TicketReasonCategories.CAT_1,
        text="Enhancements - HQ Intellect - Addition or Change to data structure, dashboard or automated report",
    ),
    TicketReason(
        id=2,
        category=TicketReasonCategories.CAT_1,
        text="Issue - HQ Intellect - Client facing feature in non-production environment broken (provide proof)",
    ),
    TicketReason(
        id=3,
        category=TicketReasonCategories.CAT_1,
        text="Infrastructure - HQ Intellect - Software Stack Change",
    ),
    TicketReason(
        id=4,
        category=TicketReasonCategories.CAT_2,
        text="Infrastructure - Report Writer - Software Stack Change",
    ),
    TicketReason(
        id=5,
        category=TicketReasonCategories.CAT_2,
        text=" Push - Report Writer - Development Push Operations from Environment to Environment",
    ),
    TicketReason(
        id=6,
        category=TicketReasonCategories.CAT_2,
        text="Enhancements - Other Product Support - Addition or Change to data structure, dashboard or automated report",
    ),
    TicketReason(
        id=7,
        category=TicketReasonCategories.CAT_3,
        text="Investigate - Determine reason and possible options for an identified view on a Non-Clinical Dashboard Project",
    ),
    TicketReason(
        id=8,
        category=TicketReasonCategories.CAT_3,
        text="Alerts - Delivered - SacValley HL7v2 Raw (Routed Raw Messages) - L-102",
        subscribers=["swarnock@konza.org", "tlamond@konza.org", "slewis@konza.org"],
    ),
    TicketReason(
        id=9,
        category=TicketReasonCategories.CAT_3,
        text="Alerts - Delivered - Corporate HL7v2 Raw (Routed Raw Messages) - L-120",
        subscribers=["tlamond@konza.org", "slewis@konza.org"],
    ),
    TicketReason(
        id=10,
        category=TicketReasonCategories.CAT_3,
        text="Alerts - Delivered - KONZA HL7v2 Raw (Routed Raw Messages) - L-52, L-10, L-82",
        subscribers=["swarnock@konza.org", "tlamond@konza.org", "slewis@konza.org"],
    ),
    TicketReason(
        id=11,
        category=TicketReasonCategories.CAT_3,
        text="CDA Retrieval from KONZA Hosted SFTP - L-69",
        subscribers=[
            "swarnock@konza.org",
            "asteffes@konza.org",
            "tlamond@konza.org",
            "jwerner@konza.org",
            "mlittlejohn@konza.org",
        ],
    ),
    TicketReason(
        id=12,
        category=TicketReasonCategories.CAT_13,
        text="Data Extract - Predefined data formatting file generation and delivery",
    ),
    TicketReason(
        id=13,
        category=TicketReasonCategories.CAT_3,
        text="DevOps Estimate - Estimate of resources/cost and timeline",
    ),
    TicketReason(
        id=14,
        category=TicketReasonCategories.CAT_3,
        text="Infrastructure - Upstream Partner Notification - Data Flow Change",
    ),
    TicketReason(
        id=15,
        category=TicketReasonCategories.CAT_3,
        text="Infrastructure - Source formatting or data structure changes",
    ),
    TicketReason(
        id=16,
        category=TicketReasonCategories.CAT_3,
        text="Routing of CCD to Destination - L-68",
        subscribers=["swarnock@konza.org"],
    ),
    TicketReason(
        id=17,
        category=TicketReasonCategories.CAT_4,
        text="Reference Database - Submit manual changes to the references tables used by the business intelligence and analytics team.",
    ),
    TicketReason(
        id=18,
        category=TicketReasonCategories.CAT_4,
        text="Population Definition - Security group definition (Patient Panel CSV, No CSV - KONZA ID or Facility Identifier, Zip code based, other method to clearly define patients of interest)",
    ),
    TicketReason(
        id=19,
        category=TicketReasonCategories.CAT_4,
        text="Facility Update - Updates to source identifier facility Names, Clinic/Non-Clinic Status, manual entry columns, and reliable distinct MRN data source updates to the facility_info or facility information tracking table",
    ),
    TicketReason(
        id=20,
        category=TicketReasonCategories.CAT_4,
        text="Population Definition - Schedule A - CSV",
    ),
    TicketReason(
        id=21,
        category=TicketReasonCategories.CAT_4,
        text="Population Definition - Schedule B - CSV",
    ),
    TicketReason(
        id=22,
        category=TicketReasonCategories.CAT_4,
        text="Population Definition - Schedule C – CSV",
    ),
    TicketReason(
        id=23,
        category=TicketReasonCategories.CAT_5,
        text="Population Definition - Security group definition (Patient Panel CSV, No CSV - KONZA ID or Facility Identifier, Zip code based, other method to clearly define patients of interest)",
    ),
    TicketReason(
        id=24,
        category=TicketReasonCategories.CAT_5,
        text="Population Definition - Schedule A - CSV",
    ),
    TicketReason(
        id=25,
        category=TicketReasonCategories.CAT_5,
        text="Population Definition - Schedule B – CSV",
    ),
    TicketReason(
        id=26,
        category=TicketReasonCategories.CAT_5,
        text="AcuteAlerts Delivery (Non-SFTP)",
    ),
    TicketReason(
        id=27,
        category=TicketReasonCategories.CAT_6,
        text="Alerts Delivery - Metric - Real Time Delivery - Delivery of aggregated alerts to be enabled for  SFTP final delivery. Delivered through Dimensional Insight application.",
    ),
    TicketReason(
        id=28,
        category=TicketReasonCategories.CAT_6,
        text="Enhancements - HQ Insights - Addition to data structure, dashboard or automated report",
    ),
    TicketReason(
        id=29,
        category=TicketReasonCategories.CAT_6,
        text="Investigate - Determine reason and possible options for an identified view on the Clinical Dashboards",
    ),
    TicketReason(
        id=30,
        category=TicketReasonCategories.CAT_6,
        text="Limit access - Hide dashboard tiles from specific user(s) or security group(s)",
    ),
    TicketReason(
        id=31,
        category=TicketReasonCategories.CAT_7,
        text="Measure definition - Codes and parameters for a metric",
    ),
    TicketReason(
        id=32,
        category=TicketReasonCategories.CAT_8,
        text="Push - Quality Team - Quality Assurance to Production",
    ),
    TicketReason(
        id=33,
        category=TicketReasonCategories.CAT_8,
        text="Maintenance - Dimensional Insight Production - One-time",
    ),
    TicketReason(
        id=33,
        category=TicketReasonCategories.CAT_8,
        text="Maintenance - Dimensional Insight Production – Recurring",
    ),
    TicketReason(
        id=34,
        category=TicketReasonCategories.CAT_9,
        text="Push - Bi-Weekly Sprints - Development Push Operations from Environment to Environment",
    ),
    TicketReason(id=35, category=TicketReasonCategories.CAT_10, text="Risk Assessment"),
    TicketReason(id=36, category=TicketReasonCategories.CAT_10, text="Internal Audit"),
    TicketReason(
        id=37,
        category=TicketReasonCategories.CAT_10,
        text="Issue - Security Project Related Item",
    ),  ##Fields to include MPI (integer and only entry at a time), exclude_reason (should include the reference/card/project)
    TicketReason(
        id=38,
        category=TicketReasonCategories.CAT_10,
        text="Certificate Management - All Sites Secured with Verified Certificates",
    ),
    TicketReason(
        id=39,
        category=TicketReasonCategories.CAT_10,
        text="External Vendor Security Audit Response",
    ),
    TicketReason(
        id=40,
        category=TicketReasonCategories.CAT_10,
        text="s3 bucket or Azure Storage Account",
    ),
    TicketReason(
        id=41,
        category=TicketReasonCategories.CAT_11,
        text="Issue - VPN or Network Connectivity Errors",
    ),
    TicketReason(
        id=42, category=TicketReasonCategories.CAT_11, text="Network Change Request"
    ),
    TicketReason(
        id=43,
        category=TicketReasonCategories.CAT_12,
        text="Information Request - Entire Organization",
    ),
    TicketReason(
        id=44, category=TicketReasonCategories.CAT_13, text="Item not listed Above"
    ),
    TicketReason(
        id=45,
        category=TicketReasonCategories.CAT_13,
        text="Issue - Internal Workstation Support (CenturyKS Logging)",
    ),
    TicketReason(
        id=46,
        category=TicketReasonCategories.CAT_13,
        text="Issue - Internal facing feature broken (provide proof)",
    ),
    TicketReason(
        id=47,
        category=TicketReasonCategories.CAT_13,
        text="Issue - Client facing feature in non-production environment broken (provide proof)",
    ),
    TicketReason(
        id=48,
        category=TicketReasonCategories.CAT_13,
        text="Extract Estimate -  Estimate of resources/cost and timeline",
    ),
    TicketReason(
        id=49,
        category=TicketReasonCategories.CAT_13,
        text="Licensing Change Request - BIA KONZA Managed Licenses Only",
    ),
    TicketReason(id=50, category=TicketReasonCategories.CAT_13, text="Equipment - New"),
    TicketReason(
        id=51,
        category=TicketReasonCategories.CAT_13,
        text="Client Projection - Estimated Volume Increase Notification",
    ),
    TicketReason(
        id=52,
        category=TicketReasonCategories.CAT_13,
        text="Equipment - Workstation Change",
    ),
    TicketReason(
        id=53, category=TicketReasonCategories.CAT_13, text="Equipment - Replacement"
    ),
    TicketReason(
        id=54,
        category=TicketReasonCategories.CAT_13,
        text="External Access Change Request - Non-Employee",
    ),
    TicketReason(
        id=55,
        category=TicketReasonCategories.CAT_13,
        text="Internal Access Change Request",
    ),
    TicketReason(
        id=56,
        category=TicketReasonCategories.CAT_13,
        text="Infrastructure - Rhapsody FHIR - Software Stack Change",
    ),
    TicketReason(
        id=57,
        category=TicketReasonCategories.CAT_13,
        text="Infrastructure as a Service - Azure - VM Support",
    ),
    TicketReason(
        id=58,
        category=TicketReasonCategories.CAT_13,
        text="Investigate - System Failure - Five Whys or Fishbone Diagram",
    ),
    TicketReason(
        id=59,
        category=TicketReasonCategories.CAT_13,
        text="Issue - Patient Privacy - Hide Records from Display",
    ),
    TicketReason(
        id=60,
        category=TicketReasonCategories.CAT_13,
        text="New Hire Equipment - New Laptop Needed - Fully ready 1 week in advance",
    ),
    TicketReason(
        id=61,
        category=TicketReasonCategories.CAT_13,
        text="New Hire Equipment - No New Laptop - Fully ready 1 week in advance",
    ),
    TicketReason(
        id=62,
        category=TicketReasonCategories.CAT_14,
        text="Issue - Internal Workstation Support (CenturyKS Logging)",
    ),
    TicketReason(
        id=63,
        category=TicketReasonCategories.NONE,
        text="SSI eHealth Exchange Distribution - L-65",
        subscribers=["aschlegel@konza.org", "jdenson@konza.org", "slewis@konza.org"],
        flag_unsubscribed_access=True,
    ),
    TicketReason(
        id=64,
        category=TicketReasonCategories.NONE,
        text="Verinovum Approval Form - L-87",
        subscribers=["tthompson@konza.org", "jdenson@konza.org"],
    ),
    # Check the next two, strangely similar to existing reason
    TicketReason(
        id=65,
        category=TicketReasonCategories.NONE,
        text="Alerts - Delivered",
        subscribers=["swarnock@konza.org"],
    ),
    TicketReason(
        id=66,
        category=TicketReasonCategories.NONE,
        text="Alerts  Raw (Routed Raw Messages) - L-52, L-10, L-82",
        subscribers=["tlamond@konza.org"],
    ),
    TicketReason(
        id=67,
        category=TicketReasonCategories.CAT_12,
        text="DevOps Estimate -  Estimate of resources/cost and timeline",
    ),
    TicketReason(
        id=68,
        category=TicketReasonCategories.CAT_15,
        text="Infrastructure - Rhapsody FHIR - Software Stack Change",
    ),
]
