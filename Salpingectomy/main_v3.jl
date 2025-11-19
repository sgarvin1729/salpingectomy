###################################################################################################################################################
# This code simulates the effect of salpingectomy on ovarian cancer mortality reduction.
#
# sim_modes:
#   - "opportunistic": salpingectomy can only occur when a qualifying abdominal surgery occurs.
#   - "non_opportunistic": salpingectomy can occur without surgery (fixed acceptance rate after age 50),
#                          and NOT if the woman has already been diagnosed.
#
# Strategies:
#   Opportunistic sim_mode:
#       - everyone: All the women take the salpingectomy at their first opportunity, regardress of their age.
#       - BTL_only: Women who takes BTL take the salpingectomy, but none of other women take salingectomy.
#       - Linear_all: No one except women taking BTL take salpingectomy until age 18, but every women take the opportunity after age 50. 
#                     Linear assumption between these two ages.
#       - Linear_half: No one except women taking BTL take salpingectomy until age 18, but half of women take the opportunity after age 50. 
#                      Linear assumption between these two ages.
#       - percent_opportunistic: at each surgery opportunity, accept with probability that can differ before vs after age 50 (pre50_acceptance, post50_acceptance).
#
#   Non-opportunistic sim_mode:
#       - percent_nonopportunistic: after age 50, woman may accept salpingectomy at a randomly chosen time between 50 and death, with probability 
#                                   nonop_acceptance_rate, but only if she has not already been diagnosed at that time.
#
# How to use:
#   1. Set `sim_mode` and `strategy` in the CONFIG section.
#   2. Adjust `pre50_acceptance`, `post50_acceptance`, and `nonop_acceptance_rate` as needed.
###################################################################################################################################################

using Distributed
num_node = 3                     
addprocs(num_node - 1)

@everywhere begin
    using CSV, DataFrames, Random
    using StatsBase, SharedArrays
    using Base.Threads

    include("./functions/everyone.jl")
    include("./functions/BTL_only.jl")
    include("./functions/Linear_all.jl")
    include("./functions/Linear_half.jl")

    # Map current strategies names to existing functions
    const curr_strats = Dict(
        "everyone"    => everyone,
        "BTL_only"    => BTL_only,
        "Linear_all"  => Linear_all,
        "Linear_half" => Linear_half,
    )

    
    # Apply salpingectomy strategy based on sim_mode and strategy

        function apply_salpingectomy_strategy(worker_rng::AbstractRNG,
                                          sim_mode::String,
                                          strategy::String,
                                          relative_risk_OvC::Float64,
                                          select_surgery::Int,
                                          cycle::Int,
                                          t_diag_raw,
                                          t_OvC_raw;
                                          pre50_acceptance::Float64 = 1.0,
                                          post50_acceptance::Float64 = 1.0,
                                          nonop_acceptance_rate::Float64 = 0.0)

        t_diag = Int(floor(t_diag_raw))
        t_OvC  = Int(floor(t_OvC_raw))

        age_years = cycle / 12


        
        # OPPORTUNISTIC STRATEGIES
        if sim_mode == "opportunistic"
            # Case 1: existing strategies
            if haskey(curr_strats, strategy)
                return curr_strats[strategy](worker_rng, relative_risk_OvC,
                                                select_surgery, cycle,
                                                t_diag, t_OvC)
            end

            # Case 2: percent-based opportunistic strategy
            if strategy == "percent_opportunistic"
                # Cannot take salpingectomy if already diagnosed
                if (t_diag > 0) && (cycle >= t_diag)
                    return false, t_OvC, 0
                end

                # Determine acceptance rate based on age
                acc_rate = age_years < 50 ? pre50_acceptance : post50_acceptance

                if rand(worker_rng) < acc_rate
                    t_salp = cycle
                    if rand(worker_rng) < (1 - relative_risk_OvC)
                        return true, 0, t_salp
                    else
                        return true, t_OvC, t_salp
                    end
                else
                    return false, t_OvC, 0
                end
            end

            error("Unknown strategy")
        end

        
        # NON-OPPORTUNISTIC STRATEGIES
        if sim_mode == "non_opportunistic"
            if strategy == "percent_nonopportunistic"
                # Non-op salpingectomy allowed only after age 50
                if age_years < 50
                    return false, t_OvC, 0
                end

                # Cannot take salpingectomy if already diagnosed
                if (t_diag > 0) && (cycle >= t_diag)
                    return false, t_OvC, 0
                end

                if rand(worker_rng) < nonop_acceptance_rate
                    t_salp = cycle

                    if rand(worker_rng) < (1 - relative_risk_OvC)
                        return true, 0, t_salp
                    else
                        return true, t_OvC, t_salp
                    end
                else
                    return false, t_OvC, 0
                end
            end

            error("Unknown strategy")
        end

        error("Unknown mode")
    end
end

# CONFIG

# Mode:
#   "opportunistic"     -> salpingectomy only at surgery times
#   "non_opportunistic" -> salpingectomy only via non-opportunistic mechanism after 50
sim_mode = "opportunistic"

# Strategy:
#   If sim_mode == "opportunistic":
#       "everyone", "BTL_only", "Linear_all", "Linear_half", "percent_opportunistic"
#   If sim_mode == "non_opportunistic":
#       "percent_nonopportunistic"
strategy = "everyone"

# For opportunistic percent strategy:
pre50_acceptance  = 1.0    # probability of accepting salpingectomy < age 50
post50_acceptance = 1.0    # probability of accepting salpingectomy ≥ age 50

# For non-opportunistic percent strategy:
nonop_acceptance_rate = 0.1  # probability after 50 of accepting a non-op salpingectomy when offered

population_size = 500
relative_risk_OvC = 0.35

println("sim_mode: ", sim_mode)
println("Strategy: ", strategy)
println("Population size: ", population_size)
println("Relative risk of OvC: ", relative_risk_OvC)
println("Pre-50 acceptance (opportunistic): ", pre50_acceptance)
println("Post-50 acceptance (opportunistic): ", post50_acceptance)
println("Non-op acceptance rate: ", nonop_acceptance_rate)


# INPUT DATA

# Possible procedures
v_procedure = ["Any procedure", "Abdominal hernia repair", "Appendectomy", "Cholecystectomy",
               "Colectomy", "Gastric bypass", "Hysterectomy", "Bilateral tubal ligation"]

# Read simulation results
sim_res = CSV.read("./inputs/simulation_results_detailed.csv", DataFrame)
if population_size < nrow(sim_res)
    sim_res = sim_res[1:population_size, :]
end
sim_res.index = collect(1:nrow(sim_res))

# Procedure rate
procedure_count = zeros(90 * 12, 8)

# Events in a year
Any_procedure = vcat(fill(0, 8*12),    fill(4626, 8*12), fill(5154, 5*12), fill(7665, 5*12), fill(7435, 5*12), fill(5763, 5*12),
                     fill(5246, 5*12), fill(4442, 5*12), fill(4157, 5*12), fill(3240, 5*12), fill(7771, 5*12), fill(6307, 5*12),
                     fill(4391, 5*12), fill(2688, 5*12), fill(1911, 14*12))
procedure_count[:, 1] = Any_procedure

Abdominal_hernia_repair = vcat(fill(0, 8*12),  fill(0, 8*12),   fill(18, 5*12), fill(54, 5*12), fill(101, 5*12), fill(168, 5*12),
                               fill(216, 5*12), fill(235, 5*12), fill(273, 5*12), fill(224, 5*12), fill(577, 5*12), fill(467, 5*12),
                               fill(313, 5*12), fill(148, 5*12), fill(84, 14*12))
procedure_count[:, 2] = Abdominal_hernia_repair

Appendectomy = vcat(fill(0, 8*12),   fill(829, 8*12), fill(438, 5*12), fill(535, 5*12), fill(571, 5*12), fill(521, 5*12),
                    fill(533, 5*12), fill(535, 5*12), fill(548, 5*12), fill(347, 5*12), fill(746, 5*12), fill(518, 5*12),
                    fill(295, 5*12), fill(164, 5*12), fill(89, 14*12))
procedure_count[:, 3] = Appendectomy

Cholecystectomy = vcat(fill(0, 8*12),   fill(1295, 8*12), fill(1271, 5*12), fill(1723, 5*12), fill(1747, 5*12), fill(1859, 5*12),
                       fill(2029, 5*12), fill(2059, 5*12), fill(2146, 5*12), fill(1616, 5*12), fill(3950, 5*12), fill(3096, 5*12),
                       fill(2030, 5*12), fill(1197, 5*12), fill(825, 14*12))
procedure_count[:, 4] = Cholecystectomy

Colectomy = vcat(fill(0, 8*12),   fill(87, 8*12),   fill(66, 5*12),  fill(74, 5*12),  fill(101, 5*12),  fill(147, 5*12),
                 fill(217, 5*12), fill(333, 5*12),  fill(440, 5*12), fill(493, 5*12), fill(1313, 5*12), fill(1400, 5*12),
                 fill(1305, 5*12), fill(1011, 5*12), fill(840, 14*12))
procedure_count[:, 5] = Colectomy

Gastric_bypass = vcat(fill(0, 8*12),   fill(19, 8*12),  fill(35, 5*12),  fill(100, 5*12), fill(119, 5*12), fill(127, 5*12),
                      fill(164, 5*12), fill(185, 5*12), fill(161, 5*12), fill(116, 5*12), fill(176, 5*12), fill(39, 5*12),
                      fill(0, 5*12),   fill(0, 5*12),   fill(0, 14*12))
procedure_count[:, 6] = Gastric_bypass

Hysterectomy = vcat(fill(0, 8*12),   fill(0, 8*12),   fill(11, 5*12),  fill(64, 5*12), fill(227, 5*12), fill(619, 5*12),
                    fill(809, 5*12), fill(428, 5*12), fill(142, 5*12), fill(93, 5*12), fill(206, 5*12), fill(187, 5*12),
                    fill(83, 5*12),  fill(40, 5*12),  fill(14, 14*12))
procedure_count[:, 7] = Hysterectomy

BTL = vcat(fill(0, 8*12),   fill(1132, 8*12), fill(1919, 5*12), fill(3158, 5*12), fill(2398, 5*12), fill(726, 5*12),
           fill(144, 5*12), fill(18, 5*12),   fill(0, 5*12),    fill(0, 5*12),    fill(0, 5*12),    fill(0, 5*12),
           fill(0, 5*12),   fill(0, 5*12),    fill(0, 14*12))
procedure_count[:, 8] = BTL

# Population by age/month
population = vcat(fill(1, 8*12),     fill(479472,8*12), fill(303952,5*12), fill(359533,5*12), fill(373973,5*12),
                  fill(359174, 5*12), fill(385985,5*12), fill(398822,5*12), fill(439411,5*12), fill(330916,5*12),
                  fill(874465,5*12), fill(753484, 5*12), fill(530157,5*12), fill(347809,5*12), fill(377749,14*12))

# Calculate monthly rate that woman takes surgery
procedure_rate_matrix = procedure_count ./ population
procedure_rate_matrix = 1 .- exp.(-procedure_rate_matrix .* (1 / 12))   # annual -> monthly


# SIMULATION

time_surgery                 = SharedArray{Int64}(zeros(Int, nrow(sim_res), 7))  # Time of each treatment 
time_salingectomy            = SharedArray{Int64}(zeros(Int, nrow(sim_res)))     # Time of salpingectomy
time_effective_salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))     # Time of effective salpingectomy
time_OvC_death_Salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))     # Time of ovarian cancer death after salpingectomy
time_OvC_death_Salpingectomy .= sim_res.time_at_OvarianDeath

@sync @distributed for individual in 1:nrow(sim_res)
    salpingectomy_done = false
    rng = MersenneTwister(1234 + individual)

    
    # OPPORTUNISTIC PHASE

    for cycle in 1:1080
        if !salpingectomy_done
            # Check if this woman takes any abdominal surgery
            rate = sum(procedure_rate_matrix[cycle, 2:8])
            rate = clamp(rate, 0.0, 1.0)
            action = sample(rng, [true, false], Weights([rate, 1 - rate]))

            if action
                select_surgery = sample(rng, collect(2:8), Weights(procedure_rate_matrix[cycle, 2:8]))

                if time_surgery[individual, select_surgery - 1] !== 0
                    # Already had this surgery type; skip this surgery
                    continue
                end

                time_surgery[individual, select_surgery - 1] = cycle

                if (sim_mode == "opportunistic") && !salpingectomy_done
                    t_diag = sim_res.time_at_diagnosis[individual]
                    t_OvC  = sim_res.time_at_OvarianDeath[individual]

                    decision, t_OvC_salp, t_eff =
                        apply_salpingectomy_strategy(
                            rng, "opportunistic", strategy,
                            relative_risk_OvC, select_surgery, cycle,
                            t_diag, t_OvC;
                            pre50_acceptance = pre50_acceptance,
                            post50_acceptance = post50_acceptance,
                        )

                    if decision
                        salpingectomy_done = true
                        time_salingectomy[individual]            = cycle
                        time_effective_salpingectomy[individual] = t_eff
                        time_OvC_death_Salpingectomy[individual] = t_OvC_salp
                    end
                end
            end
        else
            break
        end
    end

    # NON-OPPORTUNISTIC PHASE (AGE ≥ 50)
 
    if !salpingectomy_done && sim_mode == "non_opportunistic"
        time_death = max(sim_res.time_at_OvarianDeath[individual],
                         sim_res.time_at_OCMdeath[individual])

        if time_death >= 481   # only if she lives past age 50
            # Choose a candidate time between 50 and death for non-op salpingectomy
            t_candidate = sample(rng, collect(481:time_death))
            t_diag = sim_res.time_at_diagnosis[individual]
            t_OvC  = sim_res.time_at_OvarianDeath[individual]

            decision, t_OvC_salp, t_eff =
                apply_salpingectomy_strategy(
                    rng, "non_opportunistic", strategy,
                    relative_risk_OvC, 0, t_candidate,
                    t_diag, t_OvC;
                    nonop_acceptance_rate = nonop_acceptance_rate,
                )

            if decision
                salpingectomy_done = true
                time_salingectomy[individual]            = t_candidate
                time_effective_salpingectomy[individual] = t_eff
                time_OvC_death_Salpingectomy[individual] = t_OvC_salp
            end
        end
    end
end


# OUTPUT

# Summarize results
sim_res.time_salingectomy            = time_salingectomy
sim_res.time_effective_salpingectomy = time_effective_salpingectomy
sim_res.time_OvC_death_Salpingectomy = time_OvC_death_Salpingectomy

column_names = ["Abdominal hernia repair", "Appendectomy", "Cholecystectomy", "Colectomy",
                "Gastric bypass", "Hysterectomy", "Bilateral tubal ligation"]
df_surgery = DataFrame(time_surgery, column_names)

sim_res = [sim_res df_surgery]

# Save results as CSV
CSV.write("./outputs/simulation_results_$(population_size)_$(sim_mode)_$(strategy).csv", sim_res)

# Calculate mortality reduction after salpingectomy
before = filter(x -> x.time_at_OvarianDeath > 0, sim_res)
after  = filter(x -> x.time_OvC_death_Salpingectomy > 0, sim_res)

mortality_reduction = 1 - nrow(after) / nrow(before)

println("Mortality reduction: ", round(mortality_reduction, digits = 4))
