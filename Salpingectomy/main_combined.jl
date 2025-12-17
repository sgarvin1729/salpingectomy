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
rmprocs(workers())
# nworkers()
# num_node = 3
# addprocs(num_node-1)

@everywhere begin
    using CSV, DataFrames, Random
    using StatsBase, SharedArrays
    using Base.Threads
end

# TODO: I dont know if the helper functions should be here or if we should mvoe them to a separate file
@everywhere begin
    cycle_at_age(a::Int) = a*12 + 1

    #  Helper function to build non-opportunistic time weights for different distributions
    function build_nonopp_time_weights(
        dist::String,
        total_cycles::Int;
        start_age::Int,
        end_age::Union{Int,Nothing}=nothing,
        params::Dict=Dict()
    )
        w = zeros(Float64, total_cycles)
        start = cycle_at_age(start_age)
        # If end_age is nothing, go to the end of simulation
        stop  = isnothing(end_age) ? total_cycles : min(end_age*12, total_cycles)

        if dist == "Uniform"
            w[start:stop] .= 1.0

        elseif dist == "Linear"
            len = stop - start + 1
            w[start:stop] .= collect(1:len)

        elseif dist == "Exponential"
            lambda = params[:lambda]  # TODO: What should lambda be?
            for c in start:total_cycles
                del = c - start
                w[c] = exp(-lambda * del)
            end

        else
            error("Unknown non-opportunistic distribution: $dist")
        end

        return w
    end

    # Helper function to build non-opportunistic acceptance probability vector
    function build_nonopp_accept_vec(
        total_cycles::Int;
        base_rate::Float64,
        change_age::Union{Int,Nothing}=nothing,
        after_rate::Union{Float64,Nothing}=nothing
    )
        p = fill(base_rate, total_cycles)
        if !isnothing(change_age)
            idx = cycle_at_age(change_age)
            p[idx:end] .= after_rate
        end
        return p
    end
end

@everywhere begin
    include("./functions/everyone.jl")
    include("./functions/BTL_only.jl")
    include("./functions/Linear_all.jl")
    include("./functions/Linear_half.jl")
end

# Set strategy

#  Define opportunistic procedure and their base acceptance rates

opportunistic_procedure = Dict("Abdominal hernia repair" => .1,
                                "Appendectomy" => .1,
                                "Cholecystectomy" => .1,
                                "Colectomy" => .1, 
                                "Gastric bypass" => .1,
                                "Hysterectomy" => .1,
                                "Bilateral tubal ligation" => .1)
                                
strategy = "BTL_only" # Select one from ["everyone", "BTL_only", "Linear_all", "Linear_half"]



# Define non-opportunistic salpingectomy parameters

non_opportunistic_distribution = "Uniform" # Select one from ["Uniform"]
# for non-opportunistic salpingectomy, set acceptance rate
acceptance_rate = .3
# Simulation parameters
age = 50


population_size = 200
relative_risk_OvC = 0.35
println("Strategy: ", strategy)
println("Population size: ", population_size)
println("Relative risk of OvC: ", relative_risk_OvC)

# Set random seed
@everywhere const worker_rng = MersenneTwister(1234 + myid())

# Possible procedures (surgeries)
v_procedure = ["Any procedure", "Abdominal hernia repair", "Appendectomy", "Cholecystectomy", "Colectomy", 
               "Gastric bypass", "Hysterectomy", "Bilateral tubal ligation"]

# Read simulation results
#sim_res = DataFrame(CSV.File("C:\\Users\\Ethan\\Desktop\\Salpingectomy\\inputs\\simulation_results_detailed.csv"; limit=population_size))
sim_res = CSV.read("C:\\Users\\Ethan\\Desktop\\Salpingectomy_Git\\salpingectomy\\Salpingectomy\\inputs\\simulation_results_detailed.csv", DataFrame)
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

#fname = strategy
#f = getfield(Main, Symbol(fname))
#opportunistic_rates, non_opportunistic_rates = f(worker_rng, relative_risk_OvC, select_surgery, cycle, sim_res.time_at_diagnosis[individual], sim_res.time_at_OvarianDeath[individual])


strategies = Dict("everyone" =>fill(1, 7, 1080), 
                "BTL_only" => vcat(fill(0, 6, 1080), fill(1, 1, 1080)))

opportunistic_rates = strategies[strategy]

# Non-opportunistic salpingectomy parameters

total_cycles = 1080
start_cycle_non_opportunistic = cycle_at_age(age)

# Make time distribution
time_weights = build_nonopp_time_weights(
    non_opportunistic_distribution,  # "Uniform", "Linear", "Exponential"
    total_cycles;
    start_age=age,
    end_age=100,                     
    params=Dict(:lambda => 1/120.0)  # only used if Exponential
)

# age-dependent acceptance
accept_vec = build_nonopp_accept_vec(
    total_cycles;
    base_rate=acceptance_rate,
    change_age=nothing,         # set to some age (50) if you want step change
    after_rate=nothing          # set to some prob (1.0) if you want 100% after that age
)

# Overall acceptance probability
# population-level acceptance gate that guarantees exactly p_total of women ever take non-opportunistic salpingectomy, independent of lifespan
p_total = acceptance_rate

@show sum(time_weights)
@show minimum(time_weights) maximum(time_weights)
@show findfirst(>(0.0), time_weights) findlast(>(0.0), time_weights)

@show minimum(accept_vec) maximum(accept_vec)


@sync @distributed for individual in 1:nrow(sim_res)   
    salpingectomy_done = false
    worker_rng = MersenneTwister(individual)
    
    for cycle in 1:1080
        if salpingectomy_done == false
            #Check if this women takes abdominal surgery
            rate = sum(procedure_rate_matrix[cycle, 2:8])
            action = sample(worker_rng, [true, false], Weights([rate, 1-rate]))
                        
            if action == true   # Take surgery
                select_surgery = sample(worker_rng, [2:8...], Weights(procedure_rate_matrix[cycle, 2:8]))
                
                if time_surgery[individual, select_surgery-1] !== 0
                    # Don't take surgery, since the women already took the surgery before.
                    continue
                end

                time_surgery[individual, select_surgery-1] = cycle

                if salpingectomy_done == false

                    # Check if this women takes salpingectomy and the effectiveness if taking salpingectomy
                    
                    opp_acceptence = opportunistic_rates[select_surgery-1, cycle]
                    decision = sample(worker_rng, [true, false], Weights([opp_acceptence, 1 - opp_acceptence]))
                    
                    #we might not actually use this time, it's just generated blindly to improve control flow. Can be changed if you want
                    effective_salpingectomy = sample(worker_rng, [cycle, 0], Weights([1-relative_risk_OvC, relative_risk_OvC]))
                    
                    if cycle >= sim_res.time_at_diagnosis[individual]
                        decision = false
                    end
                    if sim_res.time_at_diagnosis[individual] == 0 || effective_salpingectomy == 0 || decision == false || cycle >= sim_res.time_at_diagnosis[individual]
                        time_OvC_death_Salpingectomy[individual] = sim_res.time_at_OvarianDeath[individual]
                        t_effective_treatment = 0
                    elseif effective_salpingectomy > 0
                        time_OvC_death_Salpingectomy[individual] = 0
                        t_effective_treatment = effective_salpingectomy
                    end

                    if decision == true
                        # Take salpingectomy
                        salpingectomy_done = true
                        time_salingectomy[individual] = cycle
                        time_effective_salpingectomy[individual] = t_effective_treatment
                    end
                end               
            end     
        else
            break      
        end
    end
end

# Non-opportunistic salpingectomy after some age

@sync @distributed for individual in 1:nrow(sim_res)

    rng = MersenneTwister(individual)

    # If already got salpingectomy opportunistically, skip
    if time_salingectomy[individual] != 0
        continue
    end

    # Checking if we ever accept
    if rand(rng) >= p_total
        continue
    end

    # death time handling
    raw_time_death = maximum([sim_res.time_at_OvarianDeath[individual], sim_res.time_at_OCMdeath[individual]])
    time_death = (raw_time_death == 0) ? total_cycles : min(raw_time_death, total_cycles)
    time_death_int = Int(floor(time_death))

    # must be alive past eligibility start
    if start_cycle_non_opportunistic > time_death_int
        continue
    end

    # Sample a candidate time t using the weights on the valid index set
    idxs = start_cycle_non_opportunistic:time_death_int
    w = time_weights[idxs]

    # if weights are all zero, skip
    if all(==(0.0), w)
        continue
    end

    t_salpingectomy = sample(rng, collect(idxs), Weights(w))

    # 3) Age-dependent acceptance filter (piecewise acceptance if specified)
    if rand(rng) >= accept_vec[t_salpingectomy]
        continue
    end

    time_salingectomy[individual] = t_salpingectomy

    # effectiveness check
    diag = sim_res.time_at_diagnosis[individual]
    if diag == 0 || t_salpingectomy < Int(floor(diag))
        t_eff = sample(rng, [t_salpingectomy, 0], Weights([1-relative_risk_OvC, relative_risk_OvC]))
        if t_eff > 0
            time_OvC_death_Salpingectomy[individual] = 0
            time_effective_salpingectomy[individual] = t_eff
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
CSV.write("./outputs/simulation_results_$(population_size)_$(strategy).csv", sim_res)


# Calculate mortality reduction after salpingectomy                           
before = filter(x->x.time_at_OvarianDeath > 0.0, sim_res)                                      
after  = filter(x->x.time_OvC_death_Salpingectomy > 0.0, sim_res)                                      

mortality_reduction = 1 - nrow(after)/nrow(before)

println("Reduction: ", round(mortality_reduction, digits=4))
