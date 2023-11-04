import pytest
from dbt.tests.util import read_file,check_relations_equal,run_dbt

traffic_data_with_expected_channels = [
    # Direct: Source exactly matches "(direct)" AND Medium is one of ("(not set)", "(none)")
    {
        "source": "(direct)",
        "medium": "(none)",
        "campaign": "",
        "expected_channel": "Direct"
    },
    {
        "source": "(direct)",
        "medium": "(not set)",
        "campaign": "",
        "expected_channel": "Direct"
    },
    # Cross-network: Campaign Name contains "cross-network"
    {
        "source": "some-source",
        "medium": "some-medium",
        "campaign": "some-cross-network-campaign",
        "expected_channel": "Cross-network"
    },
    {
        "source": "some-source",
        "medium": "some-medium",
        "campaign": "cross-network",
        "expected_channel": "Cross-network"
    },
    # Paid Shopping:
    #   (Source matches a list of shopping sites
    #   OR
    #   Campaign Name matches regex ^(.*(([^a-df-z]|^)shop|shopping).*)$)
    #   AND
    #   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
    {
        "source": "alibaba",
        "medium": "",
        "campaign": "",
        "expected_channel": "Paid Shopping"
    },
    {
        "source": "some-source",
        "medium": "retargeting",
        "campaign": "shopping",
        "expected_channel": "Paid Shopping"
    },
    # Paid Search:
    #   Source matches a list of search sites
    #   AND
    #   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
    {
        "source": "google",
        "medium": "ppc",
        "campaign": "",
        "expected_channel": "Paid Search"
    },
    # Paid Social:
    #   Source matches a regex list of social sites
    #   AND
    #   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
    {
        "source": "facebook",
        "medium": "retargeting",
        "campaign": "",
        "expected_channel": "Paid Social"
    },
    # Paid Video:
    #   Source matches a list of video sites
    #   AND
    #   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
    {
        "source": "youtube.com",
        "medium": "paid-something",
        "campaign": "",
        "expected_channel": "Paid Video"
    },
    # Display:
    #   Medium is one of (“display”, “banner”, “expandable”, “interstitial”, “cpm”)
    {
        "source": "youtube.com",
        "medium": "display",
        "campaign": "",
        "expected_channel": "Display"
    },
    # Paid Other:
    #   Medium matches regex ^(.*cp.*|ppc|retargeting|paid.*)$
    {
        "source": "some-source",
        "medium": "cpc",
        "campaign": "",
        "expected_channel": "Paid Other"
    },
    # Organic Shopping:
    #   Source matches a list of shopping sites
    #   OR
    #   Campaign name matches regex ^(.*(([^a-df-z]|^)shop|shopping).*)$
    {
        "source": "Google Shopping",
        "medium": "",
        "campaign": "",
        "expected_channel": "Organic Shopping"
    },
    {
        "source": "some-source",
        "medium": "",
        "campaign": "some-shopping-campaign",
        "expected_channel": "Organic Shopping"
    },
    # Organic Social:
    #   Source matches a regex list of social sites
    #   OR
    #   Medium is one of (“social”, “social-network”, “social-media”, “sm”, “social network”, “social media”)
    {
        "source": "facebook",
        "medium": "",
        "campaign": "",
        "expected_channel": "Organic Social"
    },
    {
        "source": "some-source",
        "medium": "social",
        "campaign": "",
        "expected_channel": "Organic Social"
    },
    # Organic Video:
    #   Source matches a list of video sites
    #   OR
    #   Medium matches regex ^(.*video.*)$
    {
        "source": "youtube.com",
        "medium": "",
        "campaign": "",
        "expected_channel": "Organic Video"
    },
    {
        "source": "some-source",
        "medium": "video",
        "campaign": "",
        "expected_channel": "Organic Video"
    },
    # Organic Search:
    #   Source matches a list of search sites
    #   OR
    #   Medium exactly matches organic
    {
        "source": "bing",
        "medium": "",
        "campaign": "",
        "expected_channel": "Organic Search"
    },
    {
        "source": "some-source",
        "medium": "organic",
        "campaign": "",
        "expected_channel": "Organic Search"
    },
    # Referral:
    #   Medium is one of ("referral", "app", or "link")
    {
        "source": "some-source",
        "medium": "referral",
        "campaign": "",
        "expected_channel": "Referral"
    },
    # Email:
    #   Source = email|e-mail|e_mail|e mail
    #   OR
    #   Medium = email|e-mail|e_mail|e mail
    {
        "source": "email",
        "medium": "",
        "campaign": "",
        "expected_channel": "Email"
    },
    {
        "source": "",
        "medium": "e mail",
        "campaign": "",
        "expected_channel": "Email"
    },
    # Affiliates:
    #   Medium = affiliate
    {
        "source": "some-source",
        "medium": "affiliate",
        "campaign": "",
        "expected_channel": "Affiliates"
    },
    # Audio:
    #   Medium exactly matches audio
    {
        "source": "some-source",
        "medium": "audio",
        "campaign": "",
        "expected_channel": "Audio"
    },
    # SMS:
    #   Source exactly matches sms
    #   OR
    #   Medium exactly matches sms
    {
        "source": "sms",
        "medium": "",
        "campaign": "",
        "expected_channel": "SMS"
    },
    {
        "source": "",
        "medium": "sms",
        "campaign": "",
        "expected_channel": "SMS"
    },
    # Mobile Push Notifications:
    #   Medium ends with "push"
    #   OR
    #   Medium contains "mobile" or "notification"
    #   OR
    #   Source exactly matches "firebase"
    {
        "source": "some-source",
        "medium": "something-push",
        "campaign": "",
        "expected_channel": "Mobile Push Notifications"
    },
    {
        "source": "some-source",
        "medium": "mobile-notification",
        "campaign": "",
        "expected_channel": "Mobile Push Notifications"
    },
    {
        "source": "firebase",
        "medium": "",
        "campaign": "",
        "expected_channel": "Mobile Push Notifications"
    },
    # Unassigned is the value Analytics uses when there are no other channel rules that match the event data.
    {
        "source": "some-source",
        "medium": "some-medium",
        "campaign": "some-campaign",
        "expected_channel": "Unassigned"
    },
]

# Generate the input CSV content and the expected CSV content
csv_header = "source,medium,campaign"
expected_header = "default_channel_grouping"

traffic_input_lines = [csv_header] + [
    f"{row['source']},{row['medium']},{row['campaign']}" for row in traffic_data_with_expected_channels
]

expected_csv_lines = [expected_header] + [
    row['expected_channel'] for row in traffic_data_with_expected_channels
]

# Join the lines into a single string for input and expected CSV
traffic_input = "\n".join(traffic_input_lines)
expected_csv = "\n".join(expected_csv_lines)


actual = """
with input as (
    select * from {{ref('traffic_input')}}
    left join {{ref('source_category_mapping')}} using (source)
)
select
{{default_channel_grouping('source', 'medium', 'source_category','campaign')}} as default_channel_grouping
from input
"""

class TestDefaultChannelGrouping():
    # everything that goes in the "seeds" directory (= CSV format)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "source_category_mapping.csv": read_file('../seeds/ga4_source_categories.csv'),
            "traffic_input.csv": traffic_input,
            "expected.csv": expected_csv,
        }

    # everything that goes in the "models" directory (= SQL)
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": actual,
        }
    
    # everything that goes in the "macros"
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macro_to_test.sql": read_file('../macros/default_channel_grouping.sql'),
        }
    
    def test_mock_run_and_check(self, project):
        #breakpoint()
        run_dbt(["build"])
        check_relations_equal(project.adapter, ["actual", "expected"])