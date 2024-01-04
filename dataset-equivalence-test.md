Schematic logic

<img width="423" alt="Screenshot 2024-01-03 at 21 44 40" src="https://github.com/williamqo/ai-refactor/assets/155407754/3b4c1a3e-fa7a-4e1d-b03d-c74c9cf7416c">


# generate hash from tuple example 
-- if we taking all columns, clickhouse syntax

  select 
  	cityHash64(
  		* 
  			apply(
  				x -> 
  					ifNull(toString(x),''
  				)
  			)
  	)                               as hashed_row_value
  		
  from table_name


  
