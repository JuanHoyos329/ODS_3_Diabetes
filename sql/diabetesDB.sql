-- =====================================================
-- Script SQL para crear Base de Datos diabetesDB
-- Esquema Estrella - Dataset Diabetes BRFSS 2015
-- =====================================================

-- Crear la base de datos
CREATE DATABASE IF NOT EXISTS diabetesDB 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

-- Usar la base de datos
USE diabetesDB;

-- Configuraciones importantes para integridad referencial
SET foreign_key_checks = 1;
SET sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO';

-- =====================================================
-- TABLAS DE DIMENSIONES
-- =====================================================

-- Dimensión Demographics
CREATE TABLE dim_demographics (
    demographic_id INT AUTO_INCREMENT PRIMARY KEY,
    sex INT NOT NULL COMMENT '0=Femenino, 1=Masculino',
    age_group INT NOT NULL COMMENT '1=18-24, 2=25-29, ..., 13=80+',
    education_level INT NOT NULL COMMENT '1=Nunca asistió, 6=Universidad graduado',
    income_bracket INT NOT NULL COMMENT '1=<$10k, 8=$75k+',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Índices para mejorar performance
    INDEX idx_sex (sex),
    INDEX idx_age_group (age_group),
    INDEX idx_education (education_level),
    INDEX idx_income (income_bracket)
) ENGINE=InnoDB COMMENT='Dimensión demográfica';

-- Dimensión Lifestyle
CREATE TABLE dim_lifestyle (
    lifestyle_id INT AUTO_INCREMENT PRIMARY KEY,
    smoker_status INT NOT NULL COMMENT '0=No fumador, 1=Fumador',
    physical_activity INT NOT NULL COMMENT '0=No actividad, 1=Actividad física',
    fruits_consumption INT NOT NULL COMMENT '0=No consume frutas diario, 1=Sí consume',
    vegetables_consumption INT NOT NULL COMMENT '0=No consume verduras diario, 1=Sí consume',
    heavy_alcohol_consumption INT NOT NULL COMMENT '0=No consumo excesivo, 1=Consumo excesivo',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Índices para mejorar performance
    INDEX idx_smoker (smoker_status),
    INDEX idx_activity (physical_activity),
    INDEX idx_fruits (fruits_consumption),
    INDEX idx_vegetables (vegetables_consumption),
    INDEX idx_alcohol (heavy_alcohol_consumption)
) ENGINE=InnoDB COMMENT='Dimensión de estilo de vida';

-- Dimensión Medical Conditions
CREATE TABLE dim_medical_conditions (
    medical_conditions_id INT AUTO_INCREMENT PRIMARY KEY,
    high_blood_pressure INT NOT NULL COMMENT '0=No, 1=Sí tiene presión alta',
    high_cholesterol INT NOT NULL COMMENT '0=No, 1=Sí tiene colesterol alto',
    cholesterol_check INT NOT NULL COMMENT '0=No verificado, 1=Verificado en 5 años',
    stroke_history INT NOT NULL COMMENT '0=No, 1=Sí tuvo derrame',
    heart_disease_or_attack INT NOT NULL COMMENT '0=No, 1=Sí tuvo enfermedad cardíaca/infarto',
    difficulty_walking INT NOT NULL COMMENT '0=No dificultad, 1=Sí tiene dificultad',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Índices para mejorar performance
    INDEX idx_high_bp (high_blood_pressure),
    INDEX idx_high_chol (high_cholesterol),
    INDEX idx_stroke (stroke_history),
    INDEX idx_heart (heart_disease_or_attack),
    INDEX idx_walking (difficulty_walking)
) ENGINE=InnoDB COMMENT='Dimensión de condiciones médicas';

-- Dimensión Healthcare Access
CREATE TABLE dim_healthcare_access (
    healthcare_access_id INT AUTO_INCREMENT PRIMARY KEY,
    any_healthcare_coverage INT NOT NULL COMMENT '0=Sin cobertura, 1=Con cobertura médica',
    no_doctor_due_to_cost INT NOT NULL COMMENT '0=No, 1=No fue al médico por costo',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Índices para mejorar performance
    INDEX idx_coverage (any_healthcare_coverage),
    INDEX idx_no_doctor_cost (no_doctor_due_to_cost)
) ENGINE=InnoDB COMMENT='Dimensión de acceso a servicios de salud';

-- =====================================================
-- TABLA DE HECHOS
-- =====================================================

-- Tabla de Hechos Health Records
CREATE TABLE fact_health_records (
    record_id INT AUTO_INCREMENT PRIMARY KEY,
    diabetes_status INT NOT NULL COMMENT '0=No diabetes, 1=Prediabetes, 2=Diabetes',
    bmi_value DECIMAL(5,2) NOT NULL COMMENT 'Índice de masa corporal',
    mental_health_days INT NOT NULL COMMENT 'Días con problemas de salud mental (0-30)',
    physical_health_days INT NOT NULL COMMENT 'Días con problemas físicos (0-30)', 
    general_health_score INT NOT NULL COMMENT 'Salud general (1=Excelente, 5=Pobre)',
    
    -- Foreign Keys
    demographic_id INT NOT NULL,
    lifestyle_id INT NOT NULL,
    medical_conditions_id INT NOT NULL,
    healthcare_access_id INT NOT NULL,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Definir Foreign Keys
    CONSTRAINT fk_demographics 
        FOREIGN KEY (demographic_id) 
        REFERENCES dim_demographics(demographic_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
        
    CONSTRAINT fk_lifestyle 
        FOREIGN KEY (lifestyle_id) 
        REFERENCES dim_lifestyle(lifestyle_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
        
    CONSTRAINT fk_medical_conditions 
        FOREIGN KEY (medical_conditions_id) 
        REFERENCES dim_medical_conditions(medical_conditions_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
        
    CONSTRAINT fk_healthcare_access 
        FOREIGN KEY (healthcare_access_id) 
        REFERENCES dim_healthcare_access(healthcare_access_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    
    -- Índices para mejorar performance en consultas analíticas
    INDEX idx_diabetes_status (diabetes_status),
    INDEX idx_bmi (bmi_value),
    INDEX idx_general_health (general_health_score),
    INDEX idx_demographic (demographic_id),
    INDEX idx_lifestyle (lifestyle_id),
    INDEX idx_medical (medical_conditions_id),
    INDEX idx_healthcare (healthcare_access_id),
    
    -- Índice compuesto para análisis multidimensional
    INDEX idx_analysis (diabetes_status, demographic_id, lifestyle_id),
    
    -- Constraints para validación de datos
    CONSTRAINT chk_diabetes_status CHECK (diabetes_status IN (0, 1, 2)),
    CONSTRAINT chk_bmi CHECK (bmi_value > 0 AND bmi_value <= 100),
    CONSTRAINT chk_mental_health CHECK (mental_health_days >= 0 AND mental_health_days <= 30),
    CONSTRAINT chk_physical_health CHECK (physical_health_days >= 0 AND physical_health_days <= 30),
    CONSTRAINT chk_general_health CHECK (general_health_score >= 1 AND general_health_score <= 5)
    
) ENGINE=InnoDB COMMENT='Tabla de hechos central con métricas de salud';

-- =====================================================
-- VISTAS ÚTILES PARA ANÁLISIS
-- =====================================================

-- Vista para análisis completo
CREATE VIEW vw_diabetes_analysis AS
SELECT 
    f.record_id,
    f.diabetes_status,
    CASE f.diabetes_status 
        WHEN 0 THEN 'No Diabetes'
        WHEN 1 THEN 'Prediabetes' 
        WHEN 2 THEN 'Diabetes'
    END as diabetes_label,
    f.bmi_value,
    f.mental_health_days,
    f.physical_health_days,
    f.general_health_score,
    
    -- Dimensión Demographics
    CASE d.sex WHEN 0 THEN 'Femenino' ELSE 'Masculino' END as sex,
    d.age_group,
    d.education_level,
    d.income_bracket,
    
    -- Dimensión Lifestyle
    CASE l.smoker_status WHEN 0 THEN 'No Fumador' ELSE 'Fumador' END as smoker_status,
    CASE l.physical_activity WHEN 0 THEN 'No Activo' ELSE 'Activo' END as physical_activity,
    CASE l.fruits_consumption WHEN 0 THEN 'No Consume' ELSE 'Consume Frutas' END as fruits_consumption,
    CASE l.vegetables_consumption WHEN 0 THEN 'No Consume' ELSE 'Consume Verduras' END as vegetables_consumption,
    CASE l.heavy_alcohol_consumption WHEN 0 THEN 'No Excesivo' ELSE 'Consumo Excesivo' END as alcohol_consumption,
    
    -- Dimensión Medical Conditions
    CASE m.high_blood_pressure WHEN 0 THEN 'Normal' ELSE 'Presión Alta' END as blood_pressure,
    CASE m.high_cholesterol WHEN 0 THEN 'Normal' ELSE 'Colesterol Alto' END as cholesterol,
    CASE m.stroke_history WHEN 0 THEN 'Sin Historia' ELSE 'Con Historia' END as stroke_history,
    CASE m.heart_disease_or_attack WHEN 0 THEN 'Sin Enfermedad' ELSE 'Con Enfermedad' END as heart_disease,
    
    -- Dimensión Healthcare Access
    CASE h.any_healthcare_coverage WHEN 0 THEN 'Sin Cobertura' ELSE 'Con Cobertura' END as healthcare_coverage,
    CASE h.no_doctor_due_to_cost WHEN 0 THEN 'No' ELSE 'Sí por Costo' END as no_doctor_cost
    
FROM fact_health_records f
INNER JOIN dim_demographics d ON f.demographic_id = d.demographic_id
INNER JOIN dim_lifestyle l ON f.lifestyle_id = l.lifestyle_id
INNER JOIN dim_medical_conditions m ON f.medical_conditions_id = m.medical_conditions_id
INNER JOIN dim_healthcare_access h ON f.healthcare_access_id = h.healthcare_access_id;

-- =====================================================
-- PROCEDIMIENTO PARA ESTADÍSTICAS DE LA BASE DE DATOS
-- =====================================================

DELIMITER //
CREATE PROCEDURE GetDatabaseStats()
BEGIN
    SELECT 'Estadísticas de la Base de Datos diabetesDB' as titulo;
    
    SELECT 
        TABLE_NAME as tabla,
        TABLE_ROWS as filas_aproximadas,
        ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) as tamaño_mb
    FROM information_schema.tables 
    WHERE table_schema = 'diabetesDB' 
    AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_ROWS DESC;
    
    SELECT COUNT(*) as total_registros_hechos FROM fact_health_records;
    SELECT COUNT(DISTINCT demographic_id) as total_demograficos FROM dim_demographics;
    SELECT COUNT(DISTINCT lifestyle_id) as total_estilos_vida FROM dim_lifestyle;
    SELECT COUNT(DISTINCT medical_conditions_id) as total_condiciones_medicas FROM dim_medical_conditions;
    SELECT COUNT(DISTINCT healthcare_access_id) as total_acceso_salud FROM dim_healthcare_access;
END//
DELIMITER ;

-- =====================================================
-- MOSTRAR ESTRUCTURA CREADA
-- =====================================================

-- Verificar que todas las tablas se crearon correctamente
SHOW TABLES;

-- Mostrar la estructura de cada tabla
DESCRIBE dim_demographics;
DESCRIBE dim_lifestyle;
DESCRIBE dim_medical_conditions; 
DESCRIBE dim_healthcare_access;
DESCRIBE fact_health_records;

-- Verificar las foreign keys
SELECT 
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE CONSTRAINT_SCHEMA = 'diabetesDB'
AND REFERENCED_TABLE_NAME IS NOT NULL;

-- Mensaje final
SELECT 'Base de datos diabetesDB creada exitosamente con esquema estrella!' as resultado;