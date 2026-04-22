###################################################################################################################################################
# This code simulates the effect of opportunistic salpingectomy on ovarian cancer mortality reduction. 
# Procedure:
#   1. Set strategy
#       - Everyone: All the women take the salpingectomy at their first opportunity, regardress of their age.
#       - BTL_only: Women who takes BTL take the salpingectomy, but none of other women take salingectomy.
#       - Linear_all: No one except women taking BTL take salpingectomy until age 18, but every women take the opportunity after age 50. 
#                     Linear assumption between these two ages.
#       - Linear_half: No one except women taking BTL take salpingectomy until age 18, but half of women take the opportunity after age 50. 
#                      Linear assumption between these two ages.
#
#   2. Submit this code to HPC.
#       - Calculated mortality reduction rate will be printed out.
#       - All the simulation results will be saved as CSV file under output directory.

#Input Parameters (CSV):
#       OvC_histology - Histology of ovarian cancer (0 if no cancer, string of cancer name if present)
#       time_at_OvCPrev - Time when occult (preclinical) ovarian cancer becomes present, before diagnosis (0 if no cancer)
#       time_at_diagnosis - Time when ovarian cancer is diagnosed (0 if no cancer)
#       time_at_OvarianDeath - Time this woman would die of ovarian cancer in the baseline scenario (no salpingectomy). (0 if no death from cancer)
#       time_at_OCMdeath - Time when non-ovarian cancer death occurs (0 if she dies from cancer OR if she outlives time horizon)
#       state_at_death - Encodes which absorbing state she dies in (H = healthy), I don't think this matters for this simulation

# the stragey files need to ouput a dictionary, an age, and a non-opportunistic acceptance rate vector
###################################################################################################################################################

using Distributed
num_node = 20
addprocs(num_node-1)

@everywhere begin
    using CSV, DataFrames, Random
    using StatsBase, SharedArrays
    using Base.Threads
    using Distributions
end

@everywhere begin

    function age_to_start_cycle(a::Int)   
        return (a-10)*12 + 1
    end 
    function age_to_end_cycle(a::Int)   
        return (a-9)*12
    end 
    #  Helper functions to build non-opportunistic time weights for different distributions
    function build_nonopp_weights_uniform()
        return fill(1.0, 1080)
    end

    function build_nonopp_weights_linear(
        start_prob::Float64,
        end_prob::Float64,
        start_age::Int64,
        end_age::Int64,
    )
        w = fill(0.0, 1080)
        start_cycle = age_to_start_cycle(start_age)
        end_cycle = age_to_end_cycle(end_age)
        slope = (end_prob-start_prob)/Float64((end_cycle-(start_cycle-1)))
        
        for month in 1:(start_cycle-1)
            w[month] = start_prob
        end

        for month in start_cycle:end_cycle
            w[month] = Float64((month-(start_cycle-1)))*slope + start_prob
        end

        for month in (end_cycle+1):1080
            w[month] = end_prob
        end

        return w
    end

    function build_nonopp_weights_exp(
        start_age::Int64,
        lambda::Float64,
    )
        w = fill(0.0, 1080)
        start_cycle = age_to_start_cycle(start_age)
        
        for month in 1:(start_cycle-1)
            w[month] = 1
        end
        for month in start_cycle:1080
            w[month] = exp((month-(start_cycle-1))*lambda/12)
        end
        return w
    end

    function build_nonopp_weights_jump(
        switch_age::Int64,
        start_prob::Float64,
        end_prob::Float64
    )
        w = fill(0.0, 1080)
        switch_cycle = age_to_start_cycle(switch_age)
        
        for month in 1:(switch_cycle-1)
            w[month] = start_prob
        end
        for month in switch_cycle:1080
            w[month] = end_prob
        end   
        return w
    end

    function L1_distance(
        target_vector::Matrix{Float64}, 
        starting_vector::Matrix{Float64}
    )

        dist = sum(abs.(target_vector .- starting_vector))

        return dist
    end

    function run_simulation(
        sim_res::DataFrame, 
        procedure_rate_matrix::Matrix{Float64}, 
        time_surgery::SharedMatrix{Int64},
        time_effective_salpingectomy::SharedVector{Int64},
        time_OvC_death_Salpingectomy::SharedVector{Int64}, 
        opportunistic_rates::Matrix{Float64}
    )

        @sync @distributed for individual in 1:nrow(sim_res) 
            rng = MersenneTwister(1234 + individual)
            salpingectomy_done = false
            t_diag = sim_res.time_at_diagnosis[individual]
            t_prev = sim_res.time_at_1[individual]
            hist = sim_res.OvC_histology[individual]
            raw_time_death = maximum([sim_res.time_at_OvarianDeath[individual], sim_res.time_at_OCMdeath[individual]])
            time_death = (raw_time_death == 0) ? 1080 : min(raw_time_death, 1080)
            time_death_int = Int(floor(time_death))
            max_cycle = (t_diag == 0) ? time_death_int : t_diag #last time at which woman would be able to receive a salpingectomy (even if she has cancer)
            cycle = 1

            # Convert ages to month indices 

            while cycle <= max_cycle && !salpingectomy_done
                #Check if this women takes abdominal surgery
                @views rates = procedure_rate_matrix[cycle, 2:8]
                select_surgery = sample(rng, 1:8, Weights(vcat(1 - sum(rates), rates))) # 1 = no surgery

                eligible = (t_prev == 0.0 || cycle <= Int(floor(t_prev))) && coalesce(hist == "HGSC", false)
                                
                opp_decision = false
                
                if select_surgery >= 2  # Take surgery
                    
                    if time_surgery[individual, select_surgery-1] !== 0
                        # Don't take surgery, since the women already took the surgery before.
                        cycle += 1
                        continue
                    end

                    time_surgery[individual, select_surgery-1] = cycle

                    # Check if this women takes salpingectomy and the effectiveness if taking salpingectomy
                    
                    opp_acceptance = opportunistic_rates[select_surgery-1, cycle]
                    opp_decision = sample(rng, [true, false], Weights([opp_acceptance, 1 - opp_acceptance]))

                end

                if opp_decision == true
                    salpingectomy_done = true
                    time_salpingectomy[individual] = cycle
                    
                    if eligible
                        # same code as before
                        effective_salpingectomy = sample(rng, [cycle, 0], Weights([1-failure_rate, failure_rate]))
                        if effective_salpingectomy > 0
                            time_OvC_death_Salpingectomy[individual] = 0
                            time_effective_salpingectomy[individual] = effective_salpingectomy
                        else
                            time_effective_salpingectomy[individual] = 0
                        end
                    else
                        # Not eligible, so no effect
                        time_OvC_death_Salpingectomy[individual] = sim_res.time_at_OvarianDeath[individual]
                        time_effective_salpingectomy[individual] = 0
                    end
                else
                    # Did not take salpingectomy
                    time_OvC_death_Salpingectomy[individual] = sim_res.time_at_OvarianDeath[individual]
                    time_effective_salpingectomy[individual] = 0
                end              

                cycle += 1         
            end
        end
                        
        before_count = count(>(0.0), sim_res.time_at_OvarianDeath)
        after_count  = count(>(0.0), time_OvC_death_Salpingectomy)

        mortality_reduction = 1 - after_count / before_count

        return round(mortality_reduction, digits=4)

    end 

end

twentypercenteverytenyears = zeros(1080)

for k in 1:69
    idx = 241 + 12*k
    twentypercenteverytenyears[idx] = min(0.002*k, 0.05)
end

time_weights = build_nonopp_weights_uniform() 

opp_strategy = "twentypercenteverytenyears" # Select one from ["everyone", "BTL_only", "Linear_all", "Linear_half"]
non_opp_strategy = "twentypercenteverytenyears"

#time_weights = time_weights_list[i]

population_size = 10000000
println("Strategy: ", opp_strategy)
println("Population size: ", population_size)

## Procedure rate
procedure_count = zeros(90*12, 8)

# Events in a year
Any_procedure = vcat(fill(0, 8*12),    fill(4626, 8*12), fill(5154, 5*12), fill(7665, 5*12), fill(7435, 5*12), fill(5763, 5*12), 
                    fill(5246, 5*12), fill(4442, 5*12), fill(4157, 5*12), fill(3240, 5*12), fill(7771, 5*12), fill(6307, 5*12), 
                    fill(4391, 5*12), fill(2688, 5*12), fill(1911, 14*12))
procedure_count[:,1] = Any_procedure


Abdominal_hernia_repair = vcat(fill(0, 8*12),  fill(0, 8*12),   fill(18, 5*12), fill(54, 5*12), fill(101, 5*12), fill(168, 5*12),  
                            fill(216, 5*12),fill(235, 5*12), fill(273, 5*12), fill(224, 5*12), fill(577, 5*12), fill(467, 5*12),
                            fill(313, 5*12),fill(148, 5*12), fill(84, 14*12))
procedure_count[:,2] = Abdominal_hernia_repair

Appendectomy = vcat(fill(0, 8*12),   fill(829, 8*12), fill(438, 5*12), fill(535, 5*12), fill(571, 5*12), fill(521, 5*12), 
                    fill(533, 5*12), fill(535, 5*12), fill(548, 5*12), fill(347, 5*12), fill(746, 5*12), fill(518, 5*12), 
                    fill(295, 5*12), fill(164, 5*12), fill(89, 14*12))
procedure_count[:,3] = Appendectomy

Cholecystectomy = vcat(fill(0, 8*12),   fill(1295, 8*12), fill(1271, 5*12), fill(1723, 5*12), fill(1747, 5*12), fill(1859, 5*12), 
                    fill(2029, 5*12),fill(2059, 5*12), fill(2146, 5*12), fill(1616, 5*12), fill(3950, 5*12), fill(3096, 5*12), 
                    fill(2030, 5*12),fill(1197, 5*12), fill(825, 14*12))
procedure_count[:,4] = Cholecystectomy

Colectomy = vcat(fill(0, 8*12),   fill(87, 8*12),   fill(66, 5*12),  fill(74, 5*12),  fill(101, 5*12),  fill(147, 5*12), 
                fill(217, 5*12), fill(333, 5*12),  fill(440, 5*12), fill(493, 5*12), fill(1313, 5*12), fill(1400, 5*12), 
                fill(1305, 5*12),fill(1011, 5*12), fill(840, 14*12))
procedure_count[:,5] = Colectomy

Gastric_bypass = vcat(fill(0, 8*12),   fill(19, 8*12),  fill(35, 5*12),  fill(100, 5*12), fill(119, 5*12), fill(127, 5*12), 
                    fill(164, 5*12), fill(185, 5*12), fill(161, 5*12), fill(116, 5*12), fill(176, 5*12), fill(39, 5*12), 
                    fill(0, 5*12),   fill(0, 5*12),   fill(0, 14*12))
procedure_count[:,6] = Gastric_bypass

Hysterectomy = vcat(fill(0, 8*12),   fill(0, 8*12),   fill(11, 5*12),  fill(64, 5*12), fill(227, 5*12), fill(619, 5*12), 
                    fill(809, 5*12), fill(428, 5*12), fill(142, 5*12), fill(93, 5*12), fill(206, 5*12), fill(187, 5*12), 
                    fill(83, 5*12),  fill(40, 5*12),  fill(14, 14*12))
procedure_count[:,7] = Hysterectomy

BTL = vcat(fill(0, 8*12),   fill(1132, 8*12), fill(1919, 5*12), fill(3158, 5*12), fill(2398, 5*12), fill(726, 5*12), 
        fill(144, 5*12), fill(18, 5*12),   fill(0, 5*12),    fill(0, 5*12),    fill(0, 5*12),    fill(0, 5*12), 
        fill(0, 5*12),   fill(0, 5*12),    fill(0, 14*12))
procedure_count[:,8] = BTL

# convert the number of cases to probability of taking treatment
population = vcat(fill(1, 8*12),     fill(479472,8*12), fill(303952,5*12), fill(359533,5*12), fill(373973,5*12), 
                fill(359174, 5*12),fill(385985,5*12), fill(398822,5*12), fill(439411,5*12), fill(330916,5*12),
                fill(874465,5*12), fill(753484, 5*12), fill(530157,5*12),fill(347809,5*12), fill(377749,14*12))

# Calculate monthly rate that woman takes surgery
procedure_rate_matrix = procedure_count ./ population
procedure_rate_matrix = 1 .-exp.(-(procedure_rate_matrix) .*(1/12))     # Converting annual prob. to monthly prob.
#(i,j)th entry is probability woman undergoes procedure j during cycle i

opp_strategies = Dict("everyone" =>fill(1, 7, 1080), 
                "BTL_only" => vcat(fill(0, 6, 1080), fill(1, 1, 1080)),
                "main_v2" => vcat(fill(0, 6, 1080), hcat(fill(1, 1, 480), fill(0, 1, 600))),
                "twentypercenteverytenyears" => repeat(twentypercenteverytenyears', 7, 1)
                )


opportunistic_rates = opp_strategies[opp_strategy]


println("here0")

failure_rate = 0.35
#start loop
println("here1")

# Read simulation results
sim_res = CSV.read("./inputs/simulation_results_detailed.csv", DataFrame)
#health_states = CSV.read("./inputs/matrix.csv", DataFrame)
sim_res.index = [1:nrow(sim_res)...]

# Start simulation
time_surgery                 = SharedArray{Int64}(zeros(Int, nrow(sim_res),7))      # Time of each treatment 
time_salpingectomy            = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of salpingectomy
time_effective_salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of effective salpingectomy
time_OvC_death_Salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of ovarian cancer death after salpingectomy
time_OvC_death_Salpingectomy .= sim_res.time_at_OvarianDeath

starting_point = repeat(hcat(fill(0, 1, 60), fill(0.12, 1, 120), fill(0.28, 1, 120), fill(0.35, 1, 120), fill(0.32, 1, 120), fill(0.25, 1, 120), fill(0.18, 1, 120), fill(0.12, 1, 120), fill(0.08, 1, 120), fill(0.05, 1, 60)), 7, 1)

v1 = [fill(0.28, 1, 120), fill(0.64, 1, 120), fill(1, 1, 120)]
v2 = [fill(0.35, 1, 120), fill(0.675, 1, 120), fill(1, 1, 120)]
v3 = [fill(0.32, 1, 120), fill(1, 1, 120)]
v4 = [fill(0.25, 1, 120), fill(1, 1, 120)]

directions = []

for i in 1:3
    for j in 1:3
        for k in 1:2
            for l in 1:2
                push!(directions, repeat(hcat(fill(0, 1, 60), fill(0.12, 1, 120), v1[i], v2[j], v3[k], v4[l], fill(0.18, 1, 120), fill(0.12, 1, 120), fill(0.08, 1, 120), fill(0.05, 1, 60)), 7, 1))
            end
        end
    end
end


print(length(directions))

iterations = 10

thresholds = [0.05209]

bottom_reduction = 0.0377 

for threshold in thresholds
    print("threshold is")
    print(threshold)

    for direction in directions
        time_surgery                 = SharedArray{Int64}(zeros(Int, nrow(sim_res),7))      # Time of each treatment 
        time_salpingectomy            = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of salpingectomy
        time_effective_salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of effective salpingectomy
        time_OvC_death_Salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of ovarian cancer death after salpingectomy
        time_OvC_death_Salpingectomy .= sim_res.time_at_OvarianDeath

        iter = 1

        top_rates = direction
        top_reduction = run_simulation(sim_res, procedure_rate_matrix, time_surgery, time_effective_salpingectomy, time_OvC_death_Salpingectomy, top_rates)
        bottom_rates = starting_point
        bottom_reduction = 0.0377

        while (iter <= 10)

            if (top_reduction >= threshold) && (bottom_reduction < threshold)
                time_surgery                 = SharedArray{Int64}(zeros(Int, nrow(sim_res),7))      # Time of each treatment 
                time_salpingectomy            = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of salpingectomy
                time_effective_salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of effective salpingectomy
                time_OvC_death_Salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of ovarian cancer death after salpingectomy
                time_OvC_death_Salpingectomy .= sim_res.time_at_OvarianDeath
                bottom_rates = (top_rates + bottom_rates)./2            
                bottom_reduction = run_simulation(sim_res, procedure_rate_matrix, time_surgery, time_effective_salpingectomy, time_OvC_death_Salpingectomy, bottom_rates)
            elseif (top_reduction >= threshold) && (bottom_reduction >= threshold)
                time_surgery                 = SharedArray{Int64}(zeros(Int, nrow(sim_res),7))      # Time of each treatment 
                time_salpingectomy            = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of salpingectomy
                time_effective_salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of effective salpingectomy
                time_OvC_death_Salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of ovarian cancer death after salpingectomy
                time_OvC_death_Salpingectomy .= sim_res.time_at_OvarianDeath
                if L1_distance(bottom_rates, starting_point) < L1_distance(top_rates, starting_point)  
                     top_reduction = run_simulation(sim_res, procedure_rate_matrix, time_surgery, time_effective_salpingectomy, time_OvC_death_Salpingectomy, top_rates)
                end
            else 
                print("skipped this case")
                break #binary search will never produce a feasible solution
            end

            iter += 1

        end

        if (bottom_reduction >= threshold)
            print("bottom reduction won")
            println(bottom_reduction, ", ", bottom_rates[1, 181], ", ", bottom_rates[1, 301], ", ", bottom_rates[1, 421], ", ", bottom_rates[1, 541], ", ", L1_distance(bottom_rates, starting_point))
        else
            print("top reduction won")
            println(top_reduction, ", ", top_rates[1, 181], ", ", top_rates[1, 301], ", ", top_rates[1, 421], ", ", top_rates[1, 541], ", ", L1_distance(top_rates, starting_point))
        end

    end
end




