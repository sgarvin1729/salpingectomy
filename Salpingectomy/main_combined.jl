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
num_node = 40
addprocs(num_node-1)

@everywhere begin
    using CSV, DataFrames, Random
    using StatsBase, SharedArrays
    using Base.Threads
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
                                
   
end

#time_weights_list = [build_nonopp_weights_uniform(), build_nonopp_weights_linear(0.0, 1.0, 10, 50), build_nonopp_weights_linear(0.5, 1.0, 10, 50), build_nonopp_weights_exp(10, log(2)/40), build_nonopp_weights_jump(50, 0.0, 1.0), build_nonopp_weights_jump(50, 0.25, 0.5)]

time_weights = build_nonopp_weights_uniform() 

strategy = "twopercent" # Select one from ["everyone", "BTL_only", "Linear_all", "Linear_half"]

#time_weights = time_weights_list[i]

age = 50

population_size = 10000000
relative_risk_OvC = 0.35
println("Strategy: ", strategy)
println("Population size: ", population_size)
println("Relative risk of OvC: ", relative_risk_OvC)

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

strategies = Dict("everyone" =>fill(1, 7, 1080), 
                "BTL_only" => vcat(fill(0, 6, 1080), fill(1, 1, 1080)),
                "main_v2" => vcat(fill(0, 6, 1080), hcat(fill(1, 1, 480), fill(0, 1, 600))),
                "twopercent" => vcat(fill(0.0, 240), 0.02:0.02/12:1.0, fill(1.0, 251))' .* ones(7,1)
                )

opportunistic_rates = strategies[strategy]


#acceptance_rates = [0.1, 0.2, 0.3, 0.4, 0.5]


for failure_rate in [0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75]

    # Define non-opportunistic salpingectomy parameters
    # for non-opportunistic salpingectomy, set acceptance rate
    #acceptance_rate = 0.1 #do 0.1, 0.2, 0.3, 0.4, 0.5
    #uniform 
    # linear: (0% at age 10, 100% at age 50) 
    # exponential: 
    # jump:
    # Simulation parameters

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
    num_salpingectomies          = SharedArray{Int64}(zeros(Int, 91))   

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

        while cycle <= max_cycle && !salpingectomy_done
            #Check if this women takes abdominal surgery
            @views rates = procedure_rate_matrix[cycle, 2:8]
            select_surgery = sample(rng, 1:8, Weights(vcat(1 - sum(rates), rates))) # 1 = no surgery
                            
            if select_surgery >= 2  # Take surgery
                
                if time_surgery[individual, select_surgery-1] !== 0
                    # Don't take surgery, since the women already took the surgery before.
                    cycle += 1
                    continue
                end

                time_surgery[individual, select_surgery-1] = cycle

                # Check if this women takes salpingectomy and the effectiveness if taking salpingectomy
                
                opp_acceptence = opportunistic_rates[select_surgery-1, cycle]
                decision = sample(rng, [true, false], Weights([opp_acceptence, 1 - opp_acceptence]))
            
                # Eligable only if no cancer ever or salpingectomy happens before cancer onset
                eligible = (t_prev == 0.0 || cycle <= Int(floor(t_prev))) && coalesce(hist == "HGSC", false)

                

                #health_state = health_states[individual, cycle]
                
                #eligible = (health_state = "H" || health_state = 0)

                if decision == true
                    salpingectomy_done = true
                    time_salpingectomy[individual] = cycle
                    
                    num_salpingectomies[Int(floor((cycle-1)/12))+10] += 1

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
            end   
            cycle += 1      
        end
    end

    twopercentweights = hcat(fill(0, 1, 20), collect(0.02:0.02:1)', fill(1, 1, 20))

    # Non-opportunistic salpingectomy after some age

   @sync @distributed for individual in 1:nrow(sim_res)

        
        rng = MersenneTwister(individual+1234)
        t_diag = sim_res.time_at_diagnosis[individual]
        hist = sim_res.OvC_histology[individual]

        # If already got salpingectomy opportunistically, skip
        if time_salpingectomy[individual] != 0
            continue
        end

        # Checking if we ever accept
        #if rand(rng) >= acceptance_rate
        #    continue
        #end

        # death time handling
        raw_time_death = maximum([sim_res.time_at_OvarianDeath[individual], sim_res.time_at_OCMdeath[individual]])
        time_death = (raw_time_death == 0) ? 1080 : min(raw_time_death, 1080)
        time_death_int = Int(floor(time_death))

        time_death_int_year = Int(floor((maximum([0, time_death_int-1]))/12)) + 10
        t_prev = sim_res.time_at_1[individual]

        #idxs = 1:time_death_int
        #w = time_weights[idxs]

        # if weights are all zero, skip
        #if all(==(0.0), w)
         #   continue
        #end

        age = 10

        while age <= time_death_int_year

            rate = twopercentweights[1, age - 9]

            decision = sample(rng, [true, false], Weights([rate, 1-rate]))

            possibly_effective = (t_prev == 0.0 || age_to_start_cycle(age) <= Int(floor(t_prev))) && coalesce(hist == "HGSC", false)  

            if decision
                time_salpingectomy[individual] = age_to_start_cycle(age)
                num_salpingectomies[age] += 1
                if possibly_effective
                    actually_effective = sample(rng, [true, false], Weights([1 - failure_rate, failure_rate]))
                    if actually_effective
                        time_OvC_death_Salpingectomy[individual] = 0
                        time_effective_salpingectomy[individual] = age_to_start_cycle(age)
                    end
                end
                break
            end
            
            age += 1
            
        end

        #t_salpingectomy = sample(rng, collect(idxs), Weights(w))

        # 3) Age-dependent acceptance filter (piecewise acceptance if specified)

        #=time_salpingectomy[individual] = t_salpingectomy

        t_prev = sim_res.time_at_1[individual]
        eligible = (t_prev == 0.0 || t_salpingectomy <= Int(floor(t_prev))) && coalesce(hist == "HGSC", false)  

        if eligible
            t_eff = sample(rng, [t_salpingectomy, 0], Weights([1 - failure_rate, failure_rate]))
            if t_eff > 0
                time_OvC_death_Salpingectomy[individual] = 0
                time_effective_salpingectomy[individual] = t_eff
            end
        end
        =#

        #should update time of surgery in the future, checking diagnosis time
    end

    sim_res.time_salpingectomy = time_salpingectomy
    sim_res.time_effective_salpingectomy = time_effective_salpingectomy
    sim_res.time_OvC_death_Salpingectomy = time_OvC_death_Salpingectomy

    column_names = ["Abdominal hernia repair", "Appendectomy", "Cholecystectomy", "Colectomy", 
                "Gastric bypass", "Hysterectomy", "Bilateral tubal ligation"]
    df_surgery = DataFrame(time_surgery, column_names)

    sim_res = [sim_res df_surgery]

    # Save results as CSV file
    #CSV.write("./outputs/simulation_results_combined_0.1_$(population_size)_$(strategy)_$(i).csv", sim_res)


    # Calculate mortality reduction after salpingectomy                           
    before = filter(x->x.time_at_OvarianDeath > 0.0, sim_res)                                      
    after  = filter(x->x.time_OvC_death_Salpingectomy > 0.0, sim_res)       
    
    #before_HGSC = filter(x->(x.time_at_OvarianDeath > 0.0) && (x.OvC_histology == "HGSC"), sim_res)

    #after_HGSC  = filter(x->(x.time_OvC_death_Salpingectomy > 0.0) && (x.OvC_histology == "HGSC"), sim_res)  

    mortality_reduction = 1 - nrow(after)/nrow(before)

    #mortality_reduction_HGSC = 1 - nrow(after_HGSC)/nrow(before_HGSC)

    #println("age: ", age)

    println("Reduction: ", round(mortality_reduction, digits=4))

    #if mortality_reduction <= 0.2
    #    break
    #end

    #println("Reduction (HGSC): ", round(mortality_reduction_HGSC, digits=4))

    #push!(reduction_vec, round(mortality_reduction, digits=4))

    #push!(reduction_vec_HGSC, round(mortality_reduction_HGSC, digits=4))

    
    println(round(mortality_reduction, digits=4))
    print(num_salpingectomies)

    #df = DataFrame(reduction = reduction_vec)

    #CSV.write("reduction.csv", df) 



end