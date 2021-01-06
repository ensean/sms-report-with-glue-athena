from phone_iso3166.country import phone_country
import pycountry

def parse_country_from_number(phone_number):
    """
    Parse country code from e.164 phone number
    TODO replace with https://pypi.org/project/phone-iso3166/
    """
    try:
        c = pycountry.countries.get(alpha_2=phone_country(phone_number))
        return c.name
    except Exception as e:
        return 'Unknown'


if __name__ =="__main__":
    print(parse_country_from_number('+321618665'))