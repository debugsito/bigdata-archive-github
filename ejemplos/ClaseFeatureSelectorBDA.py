#!/usr/bin/env python
# coding: utf-8

# In[134]:


from typing import List, Optional, Tuple
import numpy as np

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from pyspark.ml.feature import StringIndexer,OneHotEncoder,VectorAssembler,UnivariateFeatureSelector
from pyspark.ml import Pipeline

from pyspark.ml.stat import Correlation

from pyspark.sql.types import StringType, DateType


# In[140]:


class FeatureSelectorBDA:
    """
    Utilidades para preparación y selección de características en PySpark.

    Esta clase ayuda a:
      - Indexar la columna de etiqueta.
      - Separar columnas en categóricas y numéricas.
      - Detectar multicolinealidad entre variables numéricas.
      - Seleccionar columnas mediante `UnivariateFeatureSelector`.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame de entrada que contiene las variables y la etiqueta.
    col_label : str, default 'label'
        Nombre de la columna de etiqueta. Si es categórica, se indexará
        a una columna numérica con el mismo nombre.
    """
    
    def __init__(self,df: DataFrame, col_label: str = 'label'):
        self.df = df
        self.col_label = col_label
        self.cols_nums: list = None
        self.cols_categoricas: list = None
            
    
    def label_indexed(self)  -> DataFrame:
        """
        Indexa la columna de etiqueta categórica y la renombra como columna
        numérica con el mismo nombre de col_label.

        - La etiqueta original se conserva en <col_label>_cat.
        - La columna indexada reemplaza a col_label.

        Returns
        -------
        pyspark.sql.DataFrame
            DataFrame con las columnas <col_label>_cat (original)
            y <col_label> (indexada/numérica).
        """
        indexer_lbl = StringIndexer(inputCol=self.col_label, outputCol=f'{self.col_label}_idx', handleInvalid="skip")
        self.df = indexer_lbl.fit(self.df).transform(self.df).withColumnRenamed(self.col_label,f'{self.col_label}_cat')\
                        .withColumnRenamed(f'{self.col_label}_idx',f'{self.col_label}')
        return self.df
    
    def division_columnas(self)  -> Tuple[List[str], List[str]]:
        """
        Divide las columnas del DataFrame en categóricas (StringType)
        y numéricas (int, double, float, decimal, etc.), excluyendo
        la etiqueta y su copia categórica.

        Returns
        -------
        (list[str], list[str])
            Tupla con (cols_categoricas, cols_nums).
        """
        self.cols_categoricas = [c.name for c in self.df.schema 
                        if (c.dataType==StringType()) & (c.name!=self.col_label) & (c.name!=self.col_label+'_cat')]

        self.cols_nums = [c.name for c in self.df.schema 
                 if (c.dataType.simpleString() in ('int', 'bigint', 'double', 'float', 'decimal', 'long', 'short')) 
                     & (c.name!=self.col_label)  & (c.name!=self.col_label+'_cat')]

        return self.cols_categoricas, self.cols_nums
    
    def get_multicolinealidad(self) -> Tuple[List[str], List[str], np.ndarray]:
        
        """
        Detecta multicolinealidad entre variables numéricas usando
        la matriz de correlación de Pearson.

        Marca para descartar aquellas columnas cuyo |corr| > 0.90
        con otra columna previa en el orden de la lista.

        Requiere que division_columnas se haya ejecutado para
        poblar self.cols_nums.

        Returns
        -------
        (list[str], list[str], numpy.ndarray)
            - Lista de columnas numéricas filtradas (sin alta colinealidad).
            - Lista ordenada de columnas propuestas para eliminar.
            - Matriz de correlaciones (ndarray) en el mismo orden de
              self.cols_nums.
        """
        
        num_assembler = VectorAssembler(inputCols = self.cols_nums, outputCol='num_vec')
        df_num = num_assembler.transform(self.df).select('num_vec')
        corr_mat = Correlation.corr(df_num, "num_vec", "pearson").head()[0].toArray()

        to_drop = set()
        for i in range(len(self.cols_nums)):
            for j in range(i+1, len(self.cols_nums)):
                if abs(corr_mat[i, j]) > 0.90:
                    to_drop.add(self.cols_nums[j])

        num_cols_filtered = [c for c in self.cols_nums if c not in to_drop]
        return num_cols_filtered,sorted(list(to_drop)),corr_mat
    
    def get_cols_selected(self, featureTypeCat: bool, cols: list, 
                          threshold: float = 0.05, mode: str = 'fpr') -> list:

        # Definición de tipo de feature esperado por el selector
        featType = 'categorical' if featureTypeCat else 'continuous'

        stages = []
        if featureTypeCat:
            # Indexamos solo las columnas listadas en 'cols'
            idx_cols = [f"{c}_idx" for c in cols]
            stages += [
                StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
                for c in cols
            ]
            stages.append(VectorAssembler(inputCols=idx_cols, outputCol="vec"))
        else:
            stages.append(VectorAssembler(inputCols=cols, outputCol="vec"))

        # Pipeline para producir la columna 'vec'
        pipeline = Pipeline(stages=stages)
        df_vec = pipeline.fit(self.df).transform(self.df)

        # Selector univariado
        selector = UnivariateFeatureSelector(
            featuresCol="vec",
            labelCol=self.col_label,
            selectionMode=mode,
            outputCol="selectedFeatures"
        ).setFeatureType(featType).setLabelType("categorical").setSelectionThreshold(threshold)

        model = selector.fit(df_vec)
        selected_idx = model.selectedFeatures
        selected_cols = [cols[i] for i in selected_idx]
        return selected_cols

    def get_cols_selectednum(self, cols: list, 
                          threshold: float = 0.05, 
                          mode: str = 'fpr') -> list:

        # Definición de tipo de feature esperado por el selector

        stages = []
        stages.append(VectorAssembler(inputCols=cols, outputCol="vec"))

        # Pipeline para producir la columna 'vec'
        pipeline = Pipeline(stages=stages)
        df_vec = pipeline.fit(self.df).transform(self.df)

        # Selector univariado
        selector = UnivariateFeatureSelector(
            featuresCol="vec",
            labelCol=self.col_label,
            selectionMode=mode,
            outputCol="selectedFeatures"
        ).setFeatureType("continuous").setLabelType("continuous").setSelectionThreshold(threshold)

        model = selector.fit(df_vec)
        selected_idx = model.selectedFeatures
        selected_cols = [cols[i] for i in selected_idx]
        return selected_cols