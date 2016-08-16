#PBS -P v10
#PBS -q normal
#PBS -l walltime=05:00:00,ncpus=16,mem=6GB,jobfs=4GB
#PBS -l wd
#PBS -me

module load parallel
module use /g/data/v10/private/modules/modulefiles
module load gcc/5.2.0 core
module load openmpi/1.10.0

parallel -j 3 :::: find-lpgs_out.xml.bash

cat ls5-lpgs_out.xml.txt > ls578-lpgs_out.xml.txt
cat ls7-lpgs_out.xml.txt >> ls578-lpgs_out.xml.txt
cat ls8-lpgs_out.xml.txt >> ls578-lpgs_out.xml.txt

mpiexec -n 16 python ls_collections.py
