import pymc as pm
import numpy as np
import arviz as az

# --- 1. Вводные данные (примерные значения) ---
n_stages = 5
n_metals = 3
M_s_obs = np.random.uniform(10, 50, size=(5))
V_l_obs = np.random.uniform(10, 50, size=(4))
C_s_obs = np.random.uniform(10, 50, size=(5, n_metals))
C_l_obs = np.random.uniform(10, 50, size=(4, n_metals))
M_conc_obs = 10
M_final_obs = 100
C_conc_obs = np.random.uniform(200, 500, size=n_metals)
# Допустим, у нас есть массивы с замерами:
# M_s_obs (5,), V_l_obs (4,), C_s_obs (5,3), C_l_obs (4,3)
# M_conc_obs - масса флотоконцентрата
# M_final_obs - масса продукта на 5-й стадии (по сути M_s_obs[4])

# Погрешности лаборатории (5%)
C_s_err = C_s_obs * 0.05  
C_l_err = C_l_obs * 0.05
C_conc_err = C_conc_obs * 0.05

with pm.Model() as metal_balance_model:
    # --- А. АПРИОРНЫЕ ЗНАЧЕНИЯ ("ИСТИННЫЕ" ВЕЛИЧИНЫ) ---
    
    # Истинные массы и объемы (латентные переменные)
    # Мы центрируем их вокруг замеров (obs) с учетом ошибки весов/расходомеров (~3-5%)
    M_s_true = pm.Normal('M_s_true', mu=M_s_obs, sigma=M_s_obs * 0.03, shape=5)
    V_l_true = pm.Normal('V_l_true', mu=V_l_obs, sigma=V_l_obs * 0.03, shape=4)
    
    # Масса флотоконцентрата (выделяется на стадии 3, индекс 3)
    M_conc_true = pm.Normal('M_conc_true', mu=M_conc_obs, sigma=M_conc_obs * 0.02)
    
    # Истинные концентрации (вокруг лабораторных замеров)
    C_s_true = pm.Normal('C_s_true', mu=C_s_obs, sigma=C_s_err, shape=(5, n_metals))
    C_l_true = pm.Normal('C_l_true', mu=C_l_obs, sigma=C_l_err, shape=(4, n_metals))
    C_conc_true = pm.Normal('C_conc_true', mu=C_conc_obs, sigma=C_conc_err, shape=n_metals)

    # Потери (иерархические, как в прошлом примере)
    mu_loss = pm.Beta('mu_loss', alpha=1.5, beta=10, shape=4) # 4 перехода между 5 стадиями
    kappa_loss = pm.Exponential('kappa_loss', lam=0.1, shape=4)
    losses = pm.Beta('losses', 
                     alpha=(mu_loss * kappa_loss)[:, None], 
                     beta=((1.0 - mu_loss) * kappa_loss)[:, None], 
                     shape=(4, n_metals))

    # --- Б. РАСЧЕТ МАСС МЕТАЛЛОВ ПО ПОТОКАМ ---
    
    # Масса металла в суспензии на каждой стадии
    W_susp = M_s_true[:, None] * C_s_true + pm.math.concatenate([V_l_true[:, None] * C_l_true, np.zeros((1, n_metals))], axis=0)
    
    # Масса металла во флотоконцентрате (стадия 3)
    W_conc = M_conc_true * C_conc_true

    # --- В. УРАВНЕНИЯ МАСС-БАЛАНСА (Constraints) ---
    
    # 1. Переходы до флотации (0->1, 1->2, 2->3)
    # Металл на входе (i) * (1-потери) = Металл на выходе (i+1)
    # Но на шаге 2->3 выходом является СУММА (суспензия 3 + концентрат)
    
    # Баланс для стадий 0 и 1:
    for i in range(2):
        pm.Normal(f'bal_step_{i}', 
                  mu=W_susp[i] * (1 - losses[i]), 
                  sigma=1.0, # Жесткий штраф за несхождение
                  observed=W_susp[i+1])
        
    # Баланс для перехода 2 -> 3 (Флотация):
    # Пришло со стадии 2 = (Осталось в суспензии 3 + Ушло в концентрат)
    pm.Normal('bal_flotation',
              mu=W_susp[2] * (1 - losses[2]),
              sigma=1.0,
              observed=W_susp[3] + W_conc)
    
    # Баланс для перехода 3 -> 4 (Финальный продукт):
    # Только то, что осталось в суспензии 3, идет на финальную стадию 4
    pm.Normal('bal_final',
              mu=W_susp[3] * (1 - losses[3]),
              sigma=1.0,
              observed=W_susp[4])

    # --- Г. СЭМПЛИРОВАНИЕ ---
    trace = pm.sample(2000, tune=1000, target_accept=0.9)