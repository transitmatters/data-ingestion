from chalicelib.delays.process import alert_type
from chalicelib.delays.types import Alert

test_cases = [
    ("Red Line experiencing delays of about 10 minutes due to a disabled train at Davis", "disabled_vehicle"),
    ("Red Line experiencing delays of about 10 minutes due to a disabled trolley at Davis", "disabled_vehicle"),
    ("Red Line experiencing delays of about 10 minutes due to a train that was disabled at Davis", "disabled_vehicle"),
    ("Red Line experiencing delays of about 10 minutes due to a disabled bus at Davis", "disabled_vehicle"),
    ("Red Line experiencing delays of about 10 minutes due to a train being taken out of service", "disabled_vehicle"),
    ("Red Line experiencing delays of about 10 minutes due to a train being removed from service", "disabled_vehicle"),
    ("Orange Line trains experiencing delays due to signal problems at North Station", "signal_problem"),
    ("Orange Line trains experiencing delays due to a signal problem at North Station", "signal_problem"),
    ("Orange Line trains experiencing delays due to a signal issue at North Station", "signal_problem"),
    ("Orange Line trains experiencing delays due to signal repairs at North Station", "signal_problem"),
    ("Orange Line trains experiencing delays due to signal maintenance at North Station", "signal_problem"),
    ("Orange Line trains experiencing delays due to signal work at North Station", "signal_problem"),
    (
        "Orange Line trains experiencing delays due to work being performed by the signal department at North Station",
        "signal_problem",
    ),
    ("Framingham/Worcester Trains experiencing delays due switch problem", "switch_problem"),
    ("Framingham/Worcester Trains experiencing delays due switch issue", "switch_problem"),
    ("Salem Trains experiencing delays due witch issue", "switch_problem"),
    ("Framingham/Worcester Trains experiencing delays due switching issue", "switch_problem"),
    ("Green Line experiencing delays due to brake issue", "brake_problem"),
    ("Green Line experiencing delays due to brake issue", "brake_problem"),
    ("Green Line experiencing delays due to brake problem", "brake_problem"),
    ("Green Line experiencing delays due to brakes activated", "brake_problem"),
    ("Green Line experiencing delays due to brakes holding", "brake_problem"),
    ("Green Line experiencing delays due to brakes applied", "brake_problem"),
    ("Green Line experiencing delays due to power problem", "power_problem"),
    ("Green Line experiencing delays due to power issue", "power_problem"),
    ("Green Line experiencing delays due to overhead wires", "power_problem"),
    ("Green Line experiencing delays due to overhear wires repairs", "power_problem"),
    ("Green Line experiencing delays due to the overhead wire repairs", "power_problem"),
    ("Green Line experiencing delays due to wire repair", "power_problem"),
    ("Green Line experiencing delays due to repairs to the wire", "power_problem"),
    ("Green Line experiencing delays due to wire maintenance", "power_problem"),
    ("Green Line experiencing delays due to wire inspection", "power_problem"),
    ("Green Line experiencing delays due to wire problem", "power_problem"),
    ("Green Line experiencing delays due to electrical problem", "power_problem"),
    ("Green Line experiencing delays due to overhead catenary", "power_problem"),
    ("Green Line experiencing delays due to power department work", "power_problem"),
    ("Green Line experiencing delays due to door problem", "door_problem"),
    ("Green Line experiencing delays due to door issue", "door_problem"),
    ("Blue Line experiencing delays due to a track issue at Government Center", "track_issue"),
    ("Blue Line experiencing delays due to a track problem at Government Center", "track_issue"),
    ("Blue Line experiencing delays due to a cracked rail at Government Center", "track_issue"),
    ("Blue Line experiencing delays due to a broken rail at Government Center", "track_issue"),
    ("Blue Line experiencing delays due to a medical emergency at Government Center", "medical_emergency"),
    ("Blue Line experiencing delays due to a ill passenger at Government Center", "medical_emergency"),
    (
        "Blue Line experiencing delays due to a passenger requiring medical assistance at Government Center",
        "medical_emergency",
    ),
    (
        "Blue Line experiencing delays due to a passenger requiring medical attention at Government Center",
        "medical_emergency",
    ),
    ("Blue Line experiencing delays due to a sick passenger at Government Center", "medical_emergency"),
    ("Service delays due to flooding at Alewife", "flooding"),
    ("Delays due to police activity at Park Street", "police_activity"),
    ("Orange Line is on Fire again", "fire"),
    ("Train burning at Central", "fire"),
    ("Smoke coming from Train at Park Street", "fire"),
    ("Train being removed from service due to mechanical problem", "disabled_vehicle"),
    ("Train experiencing mechanical problem", "mechanical_problem"),
    ("Train experiencing mechanical issue", "mechanical_problem"),
    ("Train experiencing motor problem", "mechanical_problem"),
    ("Train experiencing a pantograph problem", "mechanical_problem"),
    ("Train experiencing a pantograph issue", "mechanical_problem"),
    ("Train experiencing a issue with the heating system", "mechanical_problem"),
    ("Train experiencing an air pressure problem", "mechanical_problem"),
    ("Train experiencing mechanical issues", "mechanical_problem"),
    ("Mattapan Line experiencing delays due to track work", "track_work"),
    ("Mattapan Line experiencing delays due to track maintenance", "track_work"),
    ("Mattapan Line experiencing delays due to overnight work", "track_work"),
    ("Mattapan Line experiencing delays due to track repair", "track_work"),
    ("Mattapan Line experiencing delays due to personnel performed maintenance", "track_work"),
    ("Mattapan Line experiencing delays due to maintenance work", "track_work"),
    ("Mattapan Line experiencing delays due to overnight maintenance", "track_work"),
    ("Mattapan Line experiencing delays due to single track", "track_work"),
    ("Delays caused by unauthorized vehicle on the tracks", "car_traffic"),
    ("Delays caused by vehicle blocking the tracks", "car_traffic"),
    ("Delays caused by auto accident", "car_traffic"),
    ("Delays caused by car on the tracks", "car_traffic"),
    ("Delays caused by car blocking the tracks", "car_traffic"),
    ("Delays caused by car accident", "car_traffic"),
    ("Delays caused by automobile accident", "car_traffic"),
    ("Delays caused by disabled vehicle on the tracks", "car_traffic"),
    ("Delays due to traffic", "car_traffic"),
    ("Delays caused by car blocking the track area", "car_traffic"),
    ("Delays caused by auto that was blocking", "car_traffic"),
    ("Delays caused by auto blocking the track", "car_traffic"),
    ("Delays caused by auto was removed from the track", "car_traffic"),
    ("Delays caused by accident blocking the tracks", "car_traffic"),
]


def test_specific_patterns():
    """Test specific pattern matches manually"""

    print("\n=== Testing Manual Test Cases ===\n")

    for test_text in test_cases:
        # Create a mock alert object
        mock_alert = Alert(valid_from="2024-01-01T10:00:00", valid_to="2024-01-01T10:00:00", text=test_text[0])
        actual_result = alert_type(mock_alert)
        expected_result = test_text[1]

        if actual_result != expected_result:
            print("\nFAILED TEST CASE:")
            print(f"Text: {test_text[0]}")
            print(f"Expected: {expected_result}")
            print(f"Actual: {actual_result}")
            print("-" * 80)

        assert actual_result == expected_result
