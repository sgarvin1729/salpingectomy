###################################################################################################################################################
# This code simulates the effect of opportunistic salpingectomy on ovarian cancer mortality reduction. 
# Until age 50, only women who takes BTL is going accept opportunistic salpingectomy.
# After age 50, wome with specified acceptance rate is going to accept salpingectomy. 
#
# Procedure:
#   1. Set acceptance rate.
# 
#   2. Submit this code to HPC.
#       - Calculated mortality reduction rate will be printed out.
#       - All the simulation results will be saved as CSV file under output directory.
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
    include("./functions/everyone.jl")
    include("./functions/BTL_only.jl")
    include("./functions/Linear_all.jl")
    include("./functions/Linear_half.jl")
end

# Set strategy
strategy = "BTL_only"
population_size = "10M"
relative_risk_OvC = 0.35


## Change this variable ##
acceptance_rate = 0.1           

println("Strategy: ", strategy)
println("Population size: ", population_size)
println("Relative risk of OvC: ", relative_risk_OvC)
println("Acceptance rate: ", acceptance_rate)

# Set random seed
@everywhere const worker_rng = MersenneTwister(1234 + myid())

# Possible procedures
v_procedure = ["Any procedure", "Abdominal hernia repair", "Appendectomy", "Cholecystectomy", "Colectomy", 
               "Gastric bypass", "Hysterectomy", "Bilateral tubal ligation"]

# Read simulation results
#sim_res = DataFrame(CSV.File("./inputs/simulation_results_detailed.csv"; limit=3_000_000))
sim_res = CSV.read("./inputs/simulation_results_detailed.csv", DataFrame)
sim_res.index = [1:nrow(sim_res)...]


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

 

# Start simulation
time_surgery                 = SharedArray{Int64}(zeros(Int, nrow(sim_res),7))      # Time of each treatment 
time_salingectomy            = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of salpingectomy
time_effective_salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of effective salpingectomy
time_OvC_death_Salpingectomy = SharedArray{Int64}(zeros(Int, nrow(sim_res)))        # Time of ovarian cancer death after salpingectomy
time_OvC_death_Salpingectomy .= sim_res.time_at_OvarianDeath

@sync @distributed for individual in 1:nrow(sim_res)   
    salpingectomy_done = false
    worker_rng = MersenneTwister(individual)

    # Until age 50
    for cycle in 1:480
        if salpingectomy_done == false
            
            #Check if this women takes abdominal surgery
            rate = sum(procedure_rate_matrix[cycle, 2:8])
            action = sample(worker_rng, [true, false], Weights([rate, 1-rate]))
                        
            if action == true   # Take surgery
                select_surgery = sample(worker_rng, [2:8...], Weights(procedure_rate_matrix[cycle, 2:8]))

                if time_surgery[individual, select_surgery-1] !== 0
                    # Don't take surgery, since the women already took the surgery before.
                    break
                end

                time_surgery[individual, select_surgery-1] = cycle

                if salpingectomy_done == false

                    # Check if this women takes salpingectomy and the effectiveness if taking salpingectomy
                    if strategy == "everyone"
                        decision, t_OvC_death_Salpingectomy, t_effective_treatment = everyone(worker_rng, relative_risk_OvC, select_surgery, cycle, sim_res.time_at_diagnosis[individual], sim_res.time_at_OvarianDeath[individual])
                    elseif strategy == "BTL_only"
                        decision, t_OvC_death_Salpingectomy, t_effective_treatment = BTL_only(worker_rng, relative_risk_OvC, select_surgery, cycle, sim_res.time_at_diagnosis[individual], sim_res.time_at_OvarianDeath[individual])
                    elseif strategy == "Linear_all"
                        decision, t_OvC_death_Salpingectomy, t_effective_treatment = Linear_all(worker_rng, relative_risk_OvC, select_surgery, cycle, sim_res.time_at_diagnosis[individual], sim_res.time_at_OvarianDeath[individual])
                    elseif strategy == "Linear_half"
                        decision, t_OvC_death_Salpingectomy, t_effective_treatment = Linear_half(worker_rng, relative_risk_OvC, select_surgery, cycle, sim_res.time_at_diagnosis[individual], sim_res.time_at_OvarianDeath[individual])
                    end     
                    
                    if decision == true
                        # Take salpingectomy
                        salpingectomy_done = true
                        time_salingectomy[individual] = cycle
                        time_effective_salpingectomy[individual] = t_effective_treatment
                        time_OvC_death_Salpingectomy[individual] = t_OvC_death_Salpingectomy
                    end
                end               
            end     
        else
            break      
        end
    end

    # After age 50
    if salpingectomy_done == false
        
        # Decide if this women will take salpingectomy (without opportunity) in the remaining of her life 
        flag = sample(worker_rng, [true, false], Weights([acceptance_rate, 1-acceptance_rate]))
        time_death = maximum([sim_res.time_at_OvarianDeath[individual], sim_res.time_at_OCMdeath[individual]])

        # This women take Salpingectomy after age 50
        if flag == true && 481 <= time_death
            salpingectomy_done = true
            t_salpingectomy = sample(worker_rng, [481:time_death...])
            time_salingectomy[individual] = t_salpingectomy

            # Check the effectiveness of the salpingectomy
            if t_salpingectomy < sim_res.time_at_diagnosis[individual] || sim_res.time_at_diagnosis[individual] == 0
                t_effective_salpingectomy = sample(worker_rng, [t_salpingectomy, 0], Weights([1-relative_risk_OvC, relative_risk_OvC]))
                
                if t_effective_salpingectomy > 0
                    time_OvC_death_Salpingectomy[individual] = 0
                    time_effective_salpingectomy[individual] =  t_effective_salpingectomy
                end
            end
        end
    end
end


# Summarize Results
sim_res.time_salingectomy = time_salingectomy
sim_res.time_effective_salpingectomy = time_effective_salpingectomy
sim_res.time_OvC_death_Salpingectomy = time_OvC_death_Salpingectomy

column_names = ["Abdominal hernia repair", "Appendectomy", "Cholecystectomy", "Colectomy", 
               "Gastric bypass", "Hysterectomy", "Bilateral tubal ligation"]
df_surgery = DataFrame(time_surgery, column_names)

sim_res = [sim_res df_surgery]

# Save results as CSV file
CSV.write("./outputs/simulation_results_$(population_size)_$(strategy)_$(acceptance_rate).csv", sim_res)


# Calculate mortality reduction after salpingectomy                           
before = filter(x->x.time_at_OvarianDeath > 0.0, sim_res)                                      
after  = filter(x->x.time_OvC_death_Salpingectomy > 0.0, sim_res)                                      

mortality_reduction = 1 - nrow(after)/nrow(before)

println("Reduction: ", round(mortality_reduction, digits=4))
