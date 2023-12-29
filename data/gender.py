import gender_guesser.detector as gender

#name: "surname, firstname"
# Returns: "male", "female", "unknown" or "andy" (meaning androgynous)
def get_gender(name):
    d = gender.Detector()
    firstname = name.split(',')[1].strip()
    print(firstname)
    g = d.get_gender(firstname)
    return g

""" name = "Balasz, A.J."
g = get_gender(name)

if g == "unknown" or g == "andy":
    print(f"Gender for {name} could not be determined.")
else:
    print(f"The gender of {name} is {g}.") """