
import click

a = click.option('--test')
b = click.option('--test2')

print(type(a))
print(dir(a))

@click.command()
@a
@b
def main(**kwargs):

	print(kwargs)



main()
