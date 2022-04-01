/* 
1) Quantos alunos NÃO quiseram declarar a cor/raça em 2019 (Entenda 
que o aluno marcou a opção de “não quero declarar” nessa pergunta)? 
*/
SELECT count(*) tp_cor_raca
FROM aluno
WHERE tp_cor_raca = 0;
/*
resposta 2227513
*/

/* 
2) Qual é o número de alunos do Sexo Feminino que nasceram no estado 
de código 35?
*/
SELECT count(*) alunos_sexo_feminino
FROM  aluno
WHERE tp_sexo = 1
	AND co_uf_nascimento = 35;
/*
Resposta 1004194
*/

/*
### REVER ###
3) Quantos alunos do sexo feminino no estado de código 43 ingressaram 
no ensino superior por vagas de cotas étnicas?
*/
SELECT count(*) alunos_sexo_feminino_reserva_etnica_uf_43
FROM "aluno"
WHERE "aluno"."tp_sexo" = 1
	AND "aluno"."co_uf_nascimento" = 43
	and "aluno"."in_reserva_etnico" = 1;
/*
2560
*/

/*
4) Qual é o código do estado brasileiro em que nasceram mais alunos 
matriculados em cursos de ensino superior em 2019?
*/
select co_uf_nascimento,
	count(*) qtd_nascidos
from aluno
where co_uf_nascimento is not null
group by co_uf_nascimento
order by 2 desc
limit 1;
/*
Resposta co_uf_nascimento 35 qtd_nascidos 1865517
*/

/*
5) Qual é o código do estado brasileiro (considere estado de nascimento) 
que possui o menor número de alunos cegos?
*/
select co_uf_nascimento,
	count(*) qtd_alunos_cegueira
from aluno
where co_uf_nascimento is not null
	and in_deficiencia_cegueira = 1
group by co_uf_nascimento
order by qtd_alunos_cegueira asc
limit 1;
/*
co_uf_nascimento 	qtd_alunos_cegueira
16					4
*/

/*
6) Qual é o código do estado brasileiro (considere estado de nascimento) 
que possui a maior média de idade?
*/
select avg(nu_idade) media_idade,
	co_uf_nascimento
from aluno
where co_uf_nascimento is not null
group by co_uf_nascimento
order by media_idade desc
limit 1;
/*
media_idade 		co_uf_nascimento
29.04606779580163	33
*/

/*
7) Qual é o código do estado brasileiro (considere estado de nascimento) 
que possui o SEGUNDO maior número de alunos que receberam bolsa de pesquisa?
*/
with alunos_bolsa_pesquisa_x_uf as (
	select co_uf_nascimento,
		count(*) qtd_alunos_bolsa_pesquisa
	from aluno
	where co_uf_nascimento is not null
		and in_bolsa_pesquisa = 1
	group by co_uf_nascimento
	order by qtd_alunos_bolsa_pesquisa desc
	limit 2
)
select co_uf_nascimento,
	qtd_alunos_bolsa_pesquisa
from alunos_bolsa_pesquisa_x_uf
order by qtd_alunos_bolsa_pesquisa asc
limit 1
/*
co_uf_nascimento 	qtd_alunos_bolsa_pesquisa
29					7194
*/

/*
8) Quantos alunos do sexo feminino se autodeclararam pretos?
*/
SELECT count(*) alunos_sexo_feminino
FROM aluno
WHERE tp_sexo = 1
	AND tp_cor_raca = 2
/*
alunos_sexo_feminino_cor_2
467573
*/

/*
9) Qual é o código do estado brasileiro (considere estado de nascimento)
com o SEGUNDO maior número de DOCENTES do sexo feminino?
*/
with docentes_f_group_uf as (
	select count(*) nr_docentes_feminino,
		co_uf_nascimento
	from docente
	where tp_sexo = 1
		and co_uf_nascimento is not null
	group by co_uf_nascimento
	order by nr_docentes_feminino desc
)
select rank() over (
		order by nr_docentes_feminino desc
	) rank,
	co_uf_nascimento,
	nr_docentes_feminino
from docentes_f_group_uf
order by rank
/*
rank 	co_uf_nascimento	nr_docentes_feminino
2		31					14488
*/

/*
10) Qual é a idade média dos docentes do sexo masculino?
*/
select avg(nu_idade) media_de_idade
from docente
where tp_sexo = 2
/*
media_de_idade
45.65
*/

/*
11) Qual é o código do estado brasileiro (considere estado de 
nascimento) que possui o maior número de docentes do sexo masculino 
indígenas?
*/
select co_uf_nascimento,
	count(*) nr_docentes_masc_indigenas
from docente
where tp_sexo = 2
	and tp_cor_raca = 5
	and co_uf_nascimento is not null
group by co_uf_nascimento
order by nr_docentes_masc_indigenas desc
limit 1
/*
co_uf_nascimento	nr_docentes_masc_indigenas
29					22
*/

/*
12) Qual é a diferença entre o número de docentes do sexo masculino 
que possuem doutorado e o número de docentes do sexo feminino 
que possuem doutorado?
*/
select (
			select count(*)
			from docente
			where tp_escolaridade = 5
				and tp_sexo = 2
		) 
		- 
		(
			select count(*)
			from docente
			where tp_escolaridade = 5
				and tp_sexo = 1
		) as diferenca_docentes_doutorado_sexo_M_F
/*
diferenca_docentes_doutorado_sexo
11152
*/

/*
13) Quantos cursos de DIREITO (nome do curso = ‘DIREITO’) 
são oferecidos na região NORDESTE do Brasil?
Código 	
IBGE	Estado 					Sigla
21		Maranhão 				MA
22		Piauí					PI
23		Ceará 					CE
24		Rio Grande do Norte 	RN 
25		Paraíba 				PB 
26		Pernambuco 				PE 
27		Alagoas 				AL 
28		Sergipe					SE
29		Bahia					BA
*/
select count(*) nr_cursos_direito
from curso
where no_curso like '%DIREITO%'
	and co_uf in (21, 22, 23, 24, 25, 26, 27, 28, 29)
/*
nr_cursos_direito
349
*/

/*
14) Quantos cursos superiores relacionados a dados 
	(isto é, que contém a palavra DADOS no nome do curso)
 	existem no Brasil?
*/
select count(*)
from curso
where no_curso like '%DADOS%'
/*
_col0
42
*/

/*
15) Qual é o nome do curso que contém a maior quantidade total de 
inscritos (agregue cursos com o mesmo para responder)?
*/
select count(*) quantidade,
	no_curso
from curso
group by no_curso
order by quantidade desc
limit 1
/*
quantidade 	no_curso
2317		ADMINISTRAÇÃO
*/
