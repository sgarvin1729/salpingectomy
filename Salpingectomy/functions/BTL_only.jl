function BTL_only(rng, relative_risk_OvC, select_treatmet, cycle, time_at_diagnosis, time_OvC_death)
    if cycle < time_at_diagnosis || time_at_diagnosis == 0              
        if select_treatmet == 8
            decision = true
        else
            decision = false
        end

        if decision == true
            # Check effectiveness
            if time_at_diagnosis !== 0   
                effective_salpingectomy = sample(rng, [cycle, 0], Weights([1-relative_risk_OvC, relative_risk_OvC]))
                if effective_salpingectomy > 0
                    time_OvC_death_Salpingectomy = 0
                    time_effective_treatment =  effective_salpingectomy
                else
                    time_OvC_death_Salpingectomy = time_OvC_death
                    time_effective_treatment = 0
                end
            else
                time_OvC_death_Salpingectomy = time_OvC_death
                time_effective_treatment = 0
            end
        else
            time_OvC_death_Salpingectomy = time_OvC_death
            time_effective_treatment = 0
        end

    else
        decision = false
        time_OvC_death_Salpingectomy = time_OvC_death
        time_effective_treatment = 0
    end

    return decision, time_OvC_death_Salpingectomy, time_effective_treatment
end