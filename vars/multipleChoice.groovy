import com.cwctravel.hudson.plugins.extended_choice_parameter.ExtendedChoiceParameterDefinition

def defaultParams() {
    return [
            visibleItems: 3,
            delimiter: ",",
            description: "",
            quoteValue: false
    ]
}

def call(Map params = [:]) {
    params = defaultParams() + params
    assert checkForMissingParameters(params: params, mandatory: ["name", "choices"])

    return new ExtendedChoiceParameterDefinition(
            params.name,
            "PT_MULTI_SELECT",
            params.choices.join(params.delimiter),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            false,
            params.quoteValue,
            params.visibleItems,
            params.description,
            params.delimiter)
}


def checkForMissingParameters(Map params, String[] keys) {
    def mandatoryKeys = ["params", "mandatory"]
    assert params.keySet().containsAll(mandatoryKeys) // check if all required keys were passed to checkForMissingParameters step - no fancy prints

    def missingKeys = params.mandatory.findAll({key -> !key in params.params.keySet().contains(key)})

    if (missingKeys.size() != 0) {
        error(errorMessage);
    }

    return true
}