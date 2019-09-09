r'''Make i2b2 metadata terms

see also dmtoterms.rst in `pcornet-dm`__ for design notes and tests.

__ https://bitbucket.org/gpcnetwork/pcornet-dm
'''

from typing import List, NamedTuple, Optional as Opt, Tuple

__author__ = 'Dan Connolly'
__contact__ = 'http://informatics.kumc.edu/'
__copyright__ = 'Copyright (c) 2014 University of Kansas Medical Center'
__license__ = 'MIT'
__docformat__ = "restructuredtext en"


# Columns are taken from DDL for Table I2B2
# in create_oracle_i2b2metadata_tables.sql
Term = NamedTuple('Term',
                  [('c_hlevel', int),
                   ('c_fullname', str), ('c_basecode', Opt[str]), ('c_name', str),
                   ('c_visualattributes', str),
                   ('m_applied_path', str), ('c_tooltip', Opt[bytes]),
                   ('c_synonym_cd', str),
                   ('update_date', Opt[str]), ('sourcesystem_cd', Opt[str]),
                   # Loading CLOBS from CSV doesn't seem to work.
                   # 'c_metadataxml',
                   ('c_dimcode', str), ('c_operator', str),
                   ('c_columndatatype', str),
                   ('c_columnname', str), ('c_tablename', str),
                   ('c_facttablecolumn', str)])


class I2B2MetaData(object):
    @classmethod
    def term(cls, pfx: List[str], parts: List[str], name: str,
             code: Opt[str] = None, viz: str = 'CAE',
             applies_to: List[str] = [], tooltip: Opt[str] = None,
             c_synonym_cd: str = 'N',
             update_date: Opt[str] = None,  # audit info can be added later
             sourcesystem_cd: Opt[str] = None,
             c_metadataxml: Opt[str] = None,
             c_operator: str = 'like',
             c_columndatatype: str = '@',
             c_columnname: str = 'concept_path',
             c_tablename: str = 'concept_dimension',
             c_facttablecolumn: str = 'concept_cd',
             max_tooltip_len: int = 850, encoding: str = 'utf-8') -> Term:
        r'''Make metadata for an i2b2 term.

        :param pfx: Part of the fullname that doesn't contribute to
                    the c_hlevel
        :param parts: normal parts of the c_fullname (to be joined by \)
        :param name: visible label for the term

        Remaining parameters have sensible defaults:

        >>> t1 = I2B2MetaData.term(
        ...     pfx=['', 'i2b2'], parts=['Cancer Cases', 'CS Terms'],
        ...     name='Collaborative Staging Terms')
        >>> t1
        ... # doctest: +NORMALIZE_WHITESPACE
        Term(c_hlevel=2,
             c_fullname='\\i2b2\\Cancer Cases\\CS Terms\\',
              c_basecode=None, c_name='Collaborative Staging Terms',
               c_visualattributes='CAE',
                m_applied_path='@',
                 c_tooltip='',
                  c_synonym_cd='N',
                   update_date=None,
                    sourcesystem_cd=None,
                     c_dimcode='\\i2b2\\Cancer Cases\\CS Terms\\',
                      c_operator='like', c_columndatatype='@',
                       c_columnname='concept_path',
                        c_tablename='concept_dimension',
                         c_facttablecolumn='concept_cd')

        The `c_hlevel` is taken from the length of `parts`:
        >>> print I2B2MetaData.term(
        ...     pfx=['', 'i2b2'], parts=['Cancer Cases', 'CS Terms'],
        ...      name='name').c_hlevel
        2

        The prefix and parts get combined into `c_fullname`:
        >>> print I2B2MetaData.term(
        ...     pfx=['', 'i2b2'], parts=['Cancer Cases', 'CS Terms'],
        ...     name='name').c_fullname
        \i2b2\Cancer Cases\CS Terms\

        If there's a double quote in the tooltip, the web client
        gets confused and doesn't display the tooltip.
        KLUDGE: change them to single quotes.

        >>> I2B2MetaData.term(['ROOT'], ['T'], 'T',
        ...                   tooltip='ab "x"').c_tooltip
        "ab 'x'"

        '''

        # TODO: move pfx to caller's responsibility
        # TODO: postpone serialization?
        hlevel, path = len(parts), '\\'.join(pfx + parts + [''])
        m_path = '\\'.join(applies_to + ['%']) if applies_to else '@'

        tooltip_enc = (tooltip or '')[:max_tooltip_len].encode(encoding)
        tooltip_q = tooltip_enc.replace(b'"', b"'")

        return Term(
            hlevel, path,
            code, name, viz,
            m_path, tooltip_q,
            c_synonym_cd,
            update_date, sourcesystem_cd,
            path, c_operator, c_columndatatype, c_columnname,
            c_tablename, c_facttablecolumn)

    @classmethod
    def top_term(cls, root: str) -> Term:
        return cls.term(parts=[], pfx=['', root], name=root)

    @classmethod
    def folders(cls, pfx: List[str], path_parts: List[List[str]],
                names: List[str],
                tooltips: List[Opt[str]], vizs: List[str],
                mapp: Opt[List[List[str]]] = None) -> List[Term]:
        return [cls.term(pfx=pfx,
                         parts=parts,
                         applies_to=app,
                         name=n,
                         viz=viz,
                         tooltip=tooltip)
                for (parts, n, viz, tooltip, app)
                in zip(path_parts, names, vizs, tooltips,
                       mapp or [[] for _ in path_parts])]

    @classmethod
    def discrete_terms(cls, pfx: List[str],
                       path_parts: List[List[str]], codes: List[str],
                       applies: List[List[str]], names: List[str], vizs: List[str]) -> List[Term]:
        return [
            cls.term(pfx=pfx,
                     parts=parts,
                     code=code,
                     applies_to=applies_to,
                     name=name,
                     viz=viz)
            for (parts, code, applies_to, name, viz)
            in zip(path_parts, codes, applies, names, vizs)]

    @classmethod
    def scalars(cls, pfx: List[str], field_parts: List[List[str]],
                codes: List[str],
                names: List[str], value_info: List[Tuple[str, str, str]],
                vizs: List[str],
                tooltips: List[str]) -> List[Term]:
        return [cls.term(pfx=pfx,
                         parts=parts, code=code, name=name,
                         c_columndatatype=ct,
                         c_metadataxml=cls.metadataxml(
                             datatype=datatype,
                             normalunits=units),
                         viz=viz,
                         tooltip=tooltip)
                for (parts, code, name, (ct, datatype, units), viz, tooltip)
                in zip(field_parts, codes, names, value_info, vizs, tooltips)]

    @classmethod
    def metadataxml(cls, normalunits: str,
                    datatype: str = 'Float', oktousevalues: str = 'Y',
                    # arbitrary
                    creation: str = '01/26/2011 00:00:00') -> str:
        '''
        ref: `XML Specification for Descriptive Files of Values`__

        __ https://community.i2b2.org/wiki/display/DevForum/Metadata+XML+for+Medication+Modifiers  # noqa
        '''

        # TODO? <MaxStringLength />
        #  <LowofLowValue />
        #  <HighofLowValue />
        #  <LowofHighValue />
        #  <HighofHighValue />
        #  <LowofToxicValue />
        #  <HighofToxicValue />
        #  <ConvertingUnits>
        #    <Units>cm</Units>
        #    <MultiplyingFactor>0.393701</MultiplyingFactor>
        #  </ConvertingUnits>
        return '''
        <ValueMetadata>
          <Version>3.2</Version>
          <CreationDateTime>%(CREATION)s</CreationDateTime>
          <DataType>%(DATATYPE)s</DataType>
          <Oktousevalues>%(OKTOUSEVALUES)s</Oktousevalues>
          <UnitValues>
            <NormalUnits>%(NORMALUNITS)s</NormalUnits>
          </UnitValues>
        </ValueMetadata>
        ''' % dict(CREATION=creation, DATATYPE=datatype,
                   OKTOUSEVALUES=oktousevalues,
                   NORMALUNITS=normalunits)
